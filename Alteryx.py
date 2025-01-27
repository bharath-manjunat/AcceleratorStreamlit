import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import re
import os
from io import BytesIO
from Weightage import componentWeightageDf,pluginWeightageDf


def getToolData(filePath):
    # Parse the XML

    tree = ET.parse(filePath)
    root = tree.getroot()

    data = []
    insertCounter = 0
    incrementalCounter = 0
    toolSet = []

    # Iterate through all <Node> elements in the XML
    for node in root.findall('.//Node'):

        node_data = {}

        # Extract ToolID
        tool_id = node.get('ToolID')
        if tool_id:
            node_data['ToolID'] = tool_id

        # Extract Plugin (inside GuiSettings)
        gui_settings = node.find('.//GuiSettings')
        if gui_settings is not None:
            node_data['Plugin'] = gui_settings.get('Plugin', '')

        data.append(node_data)

        # Extract Macro related data
        if gui_settings.get('Plugin') not in (
        'AlteryxBasePluginsGui.Container.Container', 'AlteryxGuiToolkit.ControlContainer.ControlContainer',
        'AlteryxGuiToolkit.ToolContainer.ToolContainer'):
            node_str = ET.tostring(node, encoding='unicode')
            matches = re.findall(r'EngineSettings\s+Macro="[^"]+"', node_str)
            for match in matches:
                #Extract only toolname from Macro
                matchPattern = re.findall(r'EngineSettings\s+Macro\s*=\s*"([^\.]+)', match)[0]
                data.append({'ToolID': tool_id, 'Plugin': matchPattern})

        # Counter to check target load
        if gui_settings.get('Plugin', ' ') in ('AlteryxBasePluginsGui.DbFileOutput.DbFileOutput'):
            if node.find('.//AppendMap') is not None:
                incrementalCounter += 1
            elif node.find('.//AppendMap') is None:
                insertCounter += 1

    df = pd.DataFrame(data)

    targetLoadDf = pd.DataFrame({'Plugin': ['Target Load - Insert Type', 'Target Load - Incremental Type'],
                                 'Tool Count': [insertCounter, incrementalCounter]})

    res = df.groupby('Plugin')['ToolID'].apply(lambda x: ','.join(map(str, x))).reset_index()
    res.columns = ['Plugin', "ToolID's"]
    res['Plugin'] = res['Plugin'].apply(lambda x: x if x.startswith('SharePoint') else x.split('.')[-1])
    res['Tool Count'] = res["ToolID's"].apply(lambda x: len(x.split(',')))

    result = pd.concat([res, targetLoadDf], ignore_index=True)

    toolSet = result['Plugin'].tolist()

    return result,toolSet


def getMaterToolList():
    unSortedToolNames = set()
    for file in fileNameList:
        _, toolName = getToolData(file)
        unSortedToolNames.update(toolName)
    toolNames = sorted({name.strip() for name in unSortedToolNames if name and name.strip()})
    return toolNames




def eachRowGeneration():
    finalDF = pd.DataFrame()
    for file in fileNameList:
        file.seek(0)
        res,_ = getToolData(file)

        pivotData = {}
        for tool in toolNames:
            toolCounts = res[res['Plugin'] == tool]['Tool Count'].tolist()
            if toolCounts:
                pivotData[tool] = toolCounts
            else:
                pivotData[tool] = [0]
        eachRow = pd.DataFrame.from_dict(pivotData, orient='columns')
        eachRow['Source File'] = file.name
        finalDF = pd.concat([finalDF, eachRow], ignore_index=True)
    return finalDF



def complexityFunction(finalDF, componentWeightageDf, pluginWeightageDf):
    finalDFMelted = finalDF.melt(id_vars='Source File', var_name='Alteryx Plug-in', value_name='Count')
    finalDFMelted['Alteryx Plug-in Lower'] = finalDFMelted['Alteryx Plug-in'].str.lower()

    finalMergedplugin = finalDFMelted.merge(pluginWeightageDf, on='Alteryx Plug-in Lower', how='left')

    pluginMergedComponent = finalMergedplugin.merge(componentWeightageDf, on='Pipeline Components Lower', how='left')

    pluginMergedComponent['Complexity Score'] = pluginMergedComponent['Count'] * pluginMergedComponent[
        'Plugin Weightage'] * pluginMergedComponent['Component Weightage']

    result = pluginMergedComponent.groupby('Source File', as_index=False)['Complexity Score'].sum()

    finalDF['Complexity Score'] = finalDF['Source File'].map(result.set_index('Source File')['Complexity Score'])




    finalDF['Total Tools'] = finalDF.drop(columns=['Source File', 'Complexity Score']).sum(axis=1)
    finalDF['Complexity'] = finalDF['Complexity Score'].apply(
        lambda x: 'Ultra Complex' if x >= 35 else 'Very Complex' if 25 <= x < 35 else 'Complex' if 18 <= x < 25 else 'Medium' if 10 <= x < 18 else 'Simple')
    finalDF['Alteryx Workflow Name'] = finalDF['Source File'].str.split('.xml').str[0]
    finalDF = finalDF[['Alteryx Workflow Name', 'Source File', 'Complexity', 'Total Tools'] + [col for col in finalDF.columns if
                                                                                 col not in ['Alteryx Workflow Name',
                                                                                             'Source File', 'Complexity',
                                                                                             'Total Tools']]]
    return finalDF


def outputGeneration():

    output = BytesIO()

    complexityDF = finalDF.groupby('Complexity')['Complexity'].count().reset_index(name='Workflow Complexity count')

    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    timestampDF = pd.DataFrame({'Analyzer Run date/Time': [currentTime]})


    toolNamesNotinDimension = [toolName for toolName in toolNames if toolName.lower() not in pluginWeightageDf['Alteryx Plug-in Lower'].values]
    missingToolDF = pd.DataFrame({"Please add weightage to the following plug-in's":toolNamesNotinDimension})


    sheet1 = 'Summary'
    sheet2 = 'Workflow_Transformation'

    with pd.ExcelWriter(output) as writer:
        timestampDF.to_excel(writer, sheet_name=sheet1, index=False, startrow=0)
        complexityDF.to_excel(writer, sheet_name=sheet1, index=False, startrow=3)
        missingToolDF.to_excel(writer, sheet_name=sheet1, index=False, startrow=10)
        finalDF.to_excel(writer, sheet_name=sheet2, index=False)

    return output






if __name__ == "__main__":
    st.title('Alteryx Accelerator')

    fileNameList = st.file_uploader('Upload XML files', type=['xml'], accept_multiple_files=True)

    if fileNameList:


        toolNames = getMaterToolList()

        finalDFbeforeComplexity = eachRowGeneration()

        finalDF = complexityFunction(finalDFbeforeComplexity, componentWeightageDf, pluginWeightageDf)

        excel_file = outputGeneration()
        st.success('Processing completed! Download your excel file below')
        st.download_button(label='Download Excel',data=excel_file,file_name='Workflow Complexity.xlsx')
