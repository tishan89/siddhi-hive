package org.wso2.carbon.siddhihive.core.utils;


//import org.apache.commons.lang.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SiddhiHiveToolBoxCreator {
//    private List<String> streamDefList;
//    private String script;
//
//    public SiddhiHiveToolBoxCreator(List<String> streamDef, String script) {
//        this.streamDefList = streamDef;
//        this.script = script;
//    }
//
//    private SiddhiHiveToolBoxCreator() {
//    }
//
//    public void createToolBox(Boolean incrementalProcessingEnabled) {
//        File parentDir = new File(Constants.TOOL_BOX_DIRECTORY);
//        parentDir.mkdirs();
//        File streamDir = new File(parentDir, Constants.STREAM_DEF_DIRECTORY);
//        streamDir.mkdirs();
//        File analyticDir = new File(parentDir, Constants.ANALYTICS_DIRECTORY);
//        analyticDir.mkdirs();
//        List<String> defName = new ArrayList<String>();
//        List<String> fileName = new ArrayList<String>();
//
//        for (int i = 0; i < streamDefList.size(); i++) {
//            fileName.add(Constants.STREAM_DEF_FILE + "_" + i);
//            defName.add(Constants.DEFN + (i + 1));
//            writeToFile(streamDir, Constants.STREAM_DEF_FILE + "_" + i, streamDefList.get(i));
//        }
//
//        String prop;
//        prop = getStreamProperties(defName, fileName, incrementalProcessingEnabled);
//        writeToFile(streamDir, Constants.PROPERTY_FILE, prop);
//
//        writeToFile(analyticDir, Constants.SCRIPT_FILE, script);
//        String analyzerProp;
//        analyzerProp = getAnalyzerProperties(Constants.SCRIPT_NAME, Constants.SCRIPT_FILE);
//        writeToFile(analyticDir, Constants.ANALYZER_PROPERTY_FILE, analyzerProp);
//
//        ZipppingUtil zipppingUtil = new ZipppingUtil();
//        zipppingUtil.zip(Constants.TOOL_BOX_DIRECTORY + ".tbox", parentDir);
//    }
//
//    public String getStreamProperties(List<String> stremDefs, List<String> fileNames, Boolean incrementalProcessingEnabled) {
//        String defList = Constants.STREAM_DEFINITIONS + "=" + StringUtils.join(stremDefs, ",");
//        List<String> properties = new ArrayList<String>();
//        for (int i = 0; i < stremDefs.size(); i++) {
//            List<String> propList = new ArrayList<String>();
//            propList.add(Constants.STREAM_DEFINITIONS + "." + stremDefs.get(i) + "." + Constants.FILE_NAME + "=" + fileNames.get(i));
//            propList.add(Constants.STREAM_DEFINITIONS + "." + stremDefs.get(i) + "." + Constants.USER_NAME + "=" + Constants.DEFAULT_USER_NAME);
//            propList.add(Constants.STREAM_DEFINITIONS + "." + stremDefs.get(i) + "." + Constants.PASSWORD + "=" + Constants.DEFAULT_PASSWORD);
//            propList.add(Constants.STREAM_DEFINITIONS + "." + stremDefs.get(i) + "." + Constants.DESCRIPTION + "=" + Constants.DEFAULT_DESCRIPTION);
//            if (incrementalProcessingEnabled) {
//                propList.add(Constants.STREAM_DEFINITIONS + "." + stremDefs.get(i) + "." + Constants.ENABLE_INCREMENTAL + "=" + Boolean.TRUE);
//            }
//            properties.add(StringUtils.join(propList, "\n"));
//        }
//        String propertyString = StringUtils.join(properties, "\n");
//
//        return defList + "\n" + propertyString;
//    }
//
//    public String getAnalyzerProperties(String scriptName, String fileName) {
//        String scriptNameLine = Constants.ANALYZER_SCRIPTS + "=" + scriptName;
//        List<String> propList = new ArrayList<String>();
//        propList.add(Constants.ANALYZER_SCRIPTS + "." + scriptName + "." + Constants.FILE_NAME + "=" + fileName);
//        propList.add(Constants.ANALYZER_SCRIPTS + "." + scriptName + "." + Constants.DESCRIPTION + "=" + Constants.DEFAULT_ANALYZER_DESCRIPTION);
//        String prop = (StringUtils.join(propList, "\n"));
//        return scriptNameLine + "\n" + prop;
//    }
//
//    private void writeToFile(File dir, String file, String content) {
//        File fileToWrite = new File(dir, file);
//        try {
//            if (!fileToWrite.exists()) {
//                fileToWrite.createNewFile();
//            }
//            FileWriter fw = new FileWriter(fileToWrite.getAbsoluteFile());
//            BufferedWriter bw = new BufferedWriter(fw);
//            bw.write(content);
//            bw.close();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}
