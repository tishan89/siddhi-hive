package org.wso2.carbon.siddhihive.core.configurations;

import org.wso2.carbon.siddhihive.core.utils.enums.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
Class to hold context information needed in processing
queries.
 */
public class Context {


    private Map<String, String> cachedValuesMap= null; //parent refernce
    private Map<String, String> selectionAttributeRenameMap = null;
    private Map<String, String> referenceIDAliasMap = null;

    private int subQueryCounter = 0;

    //enums
    private ProcessingLevel processingLevel;
    private InputStreamProcessingLevel inputStreamProcessingLevel ;
    private SelectorProcessingLevel selectorProcessingLevel;
    private WindowStreamProcessingLevel windowStreamProcessingLevel;
    private WindowProcessingLevel windowProcessingLevel;

    private Boolean isScheduled = false;

    private int timeStampCounter = 0;
    private int limitCounter = 0;



    public Context(){

        cachedValuesMap = new ConcurrentHashMap<String, String>();
        selectionAttributeRenameMap = new ConcurrentHashMap<String, String>();
        referenceIDAliasMap = new ConcurrentHashMap<String, String>();

        processingLevel = ProcessingLevel.NONE;
        inputStreamProcessingLevel = InputStreamProcessingLevel.NONE;
        selectorProcessingLevel = SelectorProcessingLevel.NONE;
        windowStreamProcessingLevel = WindowStreamProcessingLevel.NONE;
        windowProcessingLevel = WindowProcessingLevel.NONE;

    }

    public Boolean getIsScheduled() {
        return isScheduled;
    }

    public void setIsScheduled(Boolean isScheduled) {
        this.isScheduled = isScheduled;
    }

//    public void addCachedValues(String cachedID, String cachedValue) {
//        this.cachedValuesMap.put(cachedID,cachedValue);
//    }
//
//    public String getCachedValues(String cachedID ) {
//
//        String cachedValue = cachedValuesMap.get(cachedID);
//
//        if(  cachedValue != null){
//            return cachedValue;
//        }else{
//
//            if( cachedValuesMap.containsValue(cachedID) )
//                return cachedID;
//        }
//        return null;
//    }

    public String generateSubQueryIdentifier(){

        String subQueryIdentifier = "subq" + String.valueOf(++subQueryCounter);

        return subQueryIdentifier;
    }

    public String getSelectionAttributeRename(String rename) {
        return this.selectionAttributeRenameMap.get(rename);
    }

    public void addSelectionAttributeRename(String rename, String selectionString) {
        this.selectionAttributeRenameMap.put(rename, selectionString);
    }

    public ProcessingLevel getProcessingLevel() {
        return processingLevel;
    }

    public void setProcessingLevel(ProcessingLevel processingLevel) {



        this.processingLevel = processingLevel;
    }


    public SelectorProcessingLevel getSelectorProcessingLevel() {
        return selectorProcessingLevel;
    }

    public void setSelectorProcessingLevel(SelectorProcessingLevel selectorProcessingLevel) {
        this.selectorProcessingLevel = selectorProcessingLevel;
    }

    public InputStreamProcessingLevel getInputStreamProcessingLevel() {
        return inputStreamProcessingLevel;
    }

    public void setInputStreamProcessingLevel(InputStreamProcessingLevel inputStreamProcessingLevel) {
        this.inputStreamProcessingLevel = inputStreamProcessingLevel;
    }

    public WindowStreamProcessingLevel getWindowStreamProcessingLevel() {
        return windowStreamProcessingLevel;
    }

    public void setWindowStreamProcessingLevel(WindowStreamProcessingLevel windowStreamProcessingLevel) {
        this.windowStreamProcessingLevel = windowStreamProcessingLevel;
    }

    public WindowProcessingLevel getWindowProcessingLevel() {
        return windowProcessingLevel;
    }

    public void setWindowProcessingLevel(WindowProcessingLevel windowProcessingLevel) {
        this.windowProcessingLevel = windowProcessingLevel;
    }


    public String getReferenceIDAlias(String referenceID) {

        String alias = referenceIDAliasMap.get(referenceID);

        return alias;
    }

    public void setReferenceIDAlias(String referenceID, String alias) {
        this.referenceIDAliasMap.put(referenceID, alias);
    }

    public int generateTimeStampCounter(boolean generateNew){

        if(!generateNew)
            return timeStampCounter;

        return ++timeStampCounter;
    }

    public void reset(){
        timeStampCounter = 0;
        subQueryCounter = 0;
        limitCounter = 0;

        cachedValuesMap.clear();
        selectionAttributeRenameMap.clear();
        referenceIDAliasMap.clear();

        processingLevel = ProcessingLevel.NONE;
        inputStreamProcessingLevel = InputStreamProcessingLevel.NONE;
        selectorProcessingLevel = SelectorProcessingLevel.NONE;
        windowStreamProcessingLevel = WindowStreamProcessingLevel.NONE;
        windowProcessingLevel = WindowProcessingLevel.NONE;

    }


    public int generateLimitCounter(boolean generateNew){

        if(!generateNew)
            return limitCounter;

        return ++limitCounter;
    }
}
