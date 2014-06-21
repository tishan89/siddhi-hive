package org.wso2.carbon.siddhihive.core.internal;


import org.apache.log4j.Logger;
import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.headerprocessor.HeaderHandler;
import org.wso2.carbon.siddhihive.core.tablecreation.CSVTableCreator;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.tablecreation.CassandraTableCreator;
import org.wso2.carbon.siddhihive.core.tablecreation.TableCreatorBase;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.*;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
Class to manage query conversion in higher level. Will call appropriate handlers. Will also contain initial data needed for the conversion.
 */

public class SiddhiHiveManager {


    private static final Logger log = Logger.getLogger(SiddhiHiveManager.class);

    private Map<String, StreamDefinitionExt> streamDefinitionMap = null; //contains stream definition
    // private Map<String, String> queryMap= null;
    //private Map<String, String> inputStreamReferenceIDMap= null;// map to maintain both the stream ID and stream reference ID

    private Map<String, String> inputStreamGeneratedQueryMap = null; // reference ID <-----> Replacement generatedQueryID
    private Map<String, String> cachedValuesMap= null; //parent refernce
   // private Map<String, String> inputStreamGeneratedQueryMap= null; // reference ID <-----> Replacement generatedQueryID

    private ProcessingLevel processingLevel;
    private InputStreamProcessingLevel inputStreamProcessingLevel;
    private SelectorProcessingLevel selectorProcessingLevel;
    private WindowStreamProcessingLevel windowStreamProcessingLevel;
    private WindowProcessingLevel windowProcessingLevel;

    private int subQueryCounter = 0;

    private Map<String, String> selectionAttributeRenameMap = null;

    private Map<String, String> referenceIDAliasMap = null;
    private Boolean isScheduled = false;
    private Query query;


    public SiddhiHiveManager() {
        streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinitionExt>();
        //New Query Map
        // queryMap = new ConcurrentHashMap<String, String>();
        //inputStreamReferenceIDMap = new ConcurrentHashMap<String, String>();
        cachedValuesMap = new ConcurrentHashMap<String, String>();
        inputStreamGeneratedQueryMap = new ConcurrentHashMap<String, String>();
        selectionAttributeRenameMap = new ConcurrentHashMap<String, String>();
        referenceIDAliasMap = new ConcurrentHashMap<String, String>();

        Context context = new Context();
        StateManager.setContext(context);

    }

    public Map<String, StreamDefinitionExt> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setStreamDefinitionMap(Map<String, StreamDefinitionExt> streamDefinitionMap) {
        this.streamDefinitionMap = streamDefinitionMap;
    }

    public void setStreamDefinition(String streamDefinitionID, StreamDefinitionExt streamDefinition) {
        streamDefinitionMap.put(streamDefinitionID, streamDefinition);
    }

    public void setStreamDefinition(List<StreamDefinitionExt> streamDefinitionList) {
        for (StreamDefinitionExt definition : streamDefinitionList) {
            streamDefinitionMap.put(definition.getStreamDefinition().getStreamId(), definition);
        }
    }

//    public String getStreamReferenceID(String referenceID) {
//
//        String streamID = inputStreamReferenceIDMap.get(referenceID);
//
//        if(  streamID != null)
//            return streamID;
//
//        return null;
//    }

    public String getStreamGeneratedQueryID(String referenceID) {
        return inputStreamGeneratedQueryMap.get(referenceID);
    }
//    public String getStreamGeneratedQueryID(String referenceID){
//        return inputStreamGeneratedQueryMap.get(referenceID);
//    }
//
//    public void addStreamGeneratedQueryID(String referenceID, String streamGeneratedQueryID){
//         inputStreamGeneratedQueryMap.put(referenceID, streamGeneratedQueryID);
//    }

    public void addStreamGeneratedQueryID(String referenceID, String streamGeneratedQueryID) {
        inputStreamGeneratedQueryMap.put(referenceID, streamGeneratedQueryID);
    }

//    public void setInputStreamReferenceID(String referenceID, String streamID) {
//        this.inputStreamReferenceIDMap.put(referenceID, streamID);
//    }

    public void addCachedValues(String cachedID, String cachedValue) {
        this.cachedValuesMap.put(cachedID, cachedValue);
    }

    public String getCachedValues(String cachedID) {

        String cachedValue = cachedValuesMap.get(cachedID);

        if (cachedValue != null) {
            return cachedValue;
        } else {

            if (cachedValuesMap.containsValue(cachedID))
                return cachedID;
        }
        return null;
    }

    public String generateSubQueryIdentifier() {

        String subQueryIdentifier = "subq" + String.valueOf(++subQueryCounter);

        return subQueryIdentifier;
    }

    public String getSelectionAttributeRenameMap(String rename) {
        return this.selectionAttributeRenameMap.get(rename);
    }

    public void addSelectionStringMap(String rename, String selectionString) {
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

    public void removedCachedValues(String cachedID) {

        this.cachedValuesMap.remove(cachedID);
    }

    public String getReferenceIDAlias(String referenceID) {

        String alias = referenceIDAliasMap.get(referenceID);

        return alias;
    }

    public void setReferenceIDAlias(String referenceID, String alias) {
        this.referenceIDAliasMap.put(referenceID, alias);
    }

    public void setSiddhiStreamDefinition(List<StreamDefinition> streamDefinitionList) {
        for (StreamDefinition definition : streamDefinitionList) {
            for (Map.Entry<String, StreamDefinitionExt> entry : streamDefinitionMap.entrySet()) {
                if (!(definition.getStreamId().equals(entry.getKey()))) {
                    StreamDefinitionExt streamDefinition = new StreamDefinitionExt(definition.getStreamId(), definition);
                    this.setStreamDefinition(streamDefinition.getFullQualifiedStreamID(), streamDefinition);
                }
            }
        }
    }

    public StreamDefinitionExt getStreamDefinition(String streamId) {
        return streamDefinitionMap.get(streamId);
    }

    public String getQuery(Query query) {


        this.query = query;

        String hiveQuery = "";

        Context context = StateManager.getContext();

        context.setProcessingLevel(org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel.INPUT_STREAM);
        StateManager.setContext(context);
        HeaderHandler headerHandler = new HeaderHandler(this);
        Map<String, String> headerMap = headerHandler.process(query.getInputStream(), this.getStreamDefinitionMap());


        context.setProcessingLevel(org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel.INPUT_STREAM.SELECTOR);
        StateManager.setContext(context);
        QuerySelectorProcessor querySelectorProcessor = new QuerySelectorProcessor();
        querySelectorProcessor.handleSelector(query.getSelector());
        ConcurrentMap<String, String> concurrentSelectorMap = querySelectorProcessor.getSelectorQueryMap();

        context.setProcessingLevel(org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel.OUTPUT_STREAM);
        StateManager.setContext(context);
        OutStream outStream = query.getOutputStream();
        StreamDefinitionExt outStreamDefinition = getStreamDefinition(outStream.getStreamId());
        TableCreatorBase tableCreator = new CSVTableCreator();
        tableCreator.setQuery(outStreamDefinition);
        String outputInsertQuery = tableCreator.getInsertQuery();
        String outputCreate = tableCreator.getQuery();

        Stream inStream = query.getInputStream();
        List<String> lstIDs = inStream.getStreamIds();
        StreamDefinitionExt inStreamDef;

        String[] arrCreate = new String[lstIDs.size()];
        int i = 0;
        for (String s : lstIDs) {
            inStreamDef = getStreamDefinition(s);
            tableCreator = new CassandraTableCreator();
            tableCreator.setQuery(inStreamDef);
            arrCreate[i++] = tableCreator.getQuery();
        }

        String inputCreate = "";
        for (int j = 0; j < arrCreate.length; j++) {
            inputCreate += arrCreate[j];
            inputCreate += "\n";
        }
        if (headerMap.get(Constants.TIME_WINDOW_FREQUENCY) != null && !isScheduled) {
            isScheduled = true;
            schedule(Long.valueOf(headerMap.get(Constants.TIME_WINDOW_FREQUENCY)).longValue());
        } else if (concurrentSelectorMap.get(Constants.LENGTH_WINDOW_FREQUENCY) != null && !isScheduled) {
            isScheduled = true;
            schedule(Long.valueOf(headerMap.get(Constants.LENGTH_WINDOW_FREQUENCY)).longValue());
        }


        String fromClause = headerMap.get(Constants.FROM_CLAUSE);
        if (fromClause == null)
            fromClause = headerMap.get(Constants.LENGTH_WIND_FROM_QUERY);
        if (fromClause == null)
            fromClause = headerMap.get(Constants.JOIN_CLAUSE);

        String initializationScript = headerMap.get(Constants.INITALIZATION_SCRIPT);

        if (initializationScript == null)
            initializationScript = " ";

        String selectQuery = "SELECT " + concurrentSelectorMap.get(Constants.SELECTION_QUERY);
        String groupByQuery = concurrentSelectorMap.get(Constants.GROUP_BY_QUERY);

        if (groupByQuery == null)
            groupByQuery = " ";

        String havingQuery = concurrentSelectorMap.get(Constants.HAVING_QUERY);

        if (havingQuery == null)
            havingQuery = " ";

        String whereClause = headerMap.get(Constants.WHERE_CLAUSE);

        if (whereClause == null)
            whereClause = " ";

        String incrementalClause = headerMap.get(Constants.INCREMENTAL_CLAUSE);

        if (incrementalClause == null)
            incrementalClause = " ";

       // hiveQuery = outputQuery + "\n" + incrementalClause + "\n" + fromClause + "\n " + selectQuery + "\n " + groupByQuery + "\n " + havingQuery + "\n " + whereClause + "\n ";
        hiveQuery = inputCreate + "\n" + outputCreate +"\n" +outputInsertQuery + "\n" + incrementalClause + "\n" + initializationScript + "\n" + selectQuery + "\n " + fromClause + "\n " +whereClause + "\n " + groupByQuery + "\n " + havingQuery + "\n ";

        return hiveQuery;

    }

    public void schedule(long timeInMillis) {
        class ScriptTimerTask extends TimerTask {

            @Override
            public void run() {
                SiddhiHiveManager.this.getQuery(query);
            }
        }

        TimerTask timerTask = new ScriptTimerTask();
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(timerTask, timeInMillis, timeInMillis);
    }


}
