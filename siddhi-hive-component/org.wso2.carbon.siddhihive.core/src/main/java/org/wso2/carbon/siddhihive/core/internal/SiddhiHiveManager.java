package org.wso2.carbon.siddhihive.core.internal;


import org.apache.log4j.Logger;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.headerprocessor.HeaderHandler;
import org.wso2.carbon.siddhihive.core.tablecreation.CSVTableCreator;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.tablecreation.CassandraTableCreator;
import org.wso2.carbon.siddhihive.core.tablecreation.TableCreatorBase;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.ProcessingMode;
import org.wso2.carbon.siddhihive.core.utils.WindowProcessingState;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
Class to manage query conversion in higher level. Will call appropriate handlers. Will also contain initial data needed for the conversion.
 */

public class SiddhiHiveManager {


    private static final Logger log = Logger.getLogger(SiddhiHiveManager.class);

    private Map<String, StreamDefinitionExt> streamDefinitionMap= null; //contains stream definition
    private Map<String, String> queryMap= null;
    private Map<String, String> inputStreamReferenceIDMap= null;// map to maintain both the stream ID and stream reference ID

    private Map<String, String> cachedValuesMap= null; //parent refernce
    private Map<String, String> inputStreamGeneratedQueryMap= null; // reference ID <-----> Replacement generatedQueryID

    private ProcessingMode processingMode;
    private WindowProcessingState windowProcessingState;


    private ConcurrentMap<String, String> selectionAttributeRenameMap = null;

   public SiddhiHiveManager() {
        streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinitionExt>();
        //New Query Map
        queryMap = new ConcurrentHashMap<String, String>();
        inputStreamReferenceIDMap = new ConcurrentHashMap<String, String>();
        cachedValuesMap = new ConcurrentHashMap<String, String>();
        inputStreamGeneratedQueryMap =  new ConcurrentHashMap<String, String>();
        selectionAttributeRenameMap = new ConcurrentHashMap<String, String>();

        windowProcessingState = WindowProcessingState.NONE;
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

    public String getStreamReferenceID(String referenceID) {

        String streamID = inputStreamReferenceIDMap.get(referenceID);

        if(  streamID != null)
            return streamID;

        return null;
    }

    public String getStreamGeneratedQueryID(String referenceID){
        return inputStreamGeneratedQueryMap.get(referenceID);
    }

    public void addStreamGeneratedQueryID(String referenceID, String streamGeneratedQueryID){
         inputStreamGeneratedQueryMap.put(referenceID, streamGeneratedQueryID);
    }

    public void setInputStreamReferenceID(String referenceID, String streamID) {
        this.inputStreamReferenceIDMap.put(referenceID, streamID);
    }

    public void addCachedValues(String cachedID, String cachedValue) {
        this.cachedValuesMap.put(cachedID,cachedValue);
    }

    public String getCachedValues(String cachedID ) {

        String cachedValue = cachedValuesMap.get(cachedID);

        if(  cachedValue != null){
            return cachedValue;
        }else{

            if( cachedValuesMap.containsValue(cachedID) )
                return cachedID;
        }
        return null;
    }

    public String getSelectionAttributeRenameMap(String rename) {
        return this.selectionAttributeRenameMap.get(rename);
    }

    public void addSelectionStringMap(String rename, String selectionString) {
        this.selectionAttributeRenameMap.put(rename, selectionString);
    }

    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    public void setProcessingMode(ProcessingMode processingMode) {
        this.processingMode = processingMode;
    }

    public WindowProcessingState getWindowProcessingState() {
        return windowProcessingState;
    }

    public void setWindowProcessingState(WindowProcessingState windowProcessingState) {
        this.windowProcessingState = windowProcessingState;
    }

    public void removedCachedValues(String cachedID) {
        this.cachedValuesMap.remove(cachedID);
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


        String hiveQuery = "";

        setProcessingMode(ProcessingMode.INPUT_STREAM.INPUT_STREAM);
        HeaderHandler headerHandler = new HeaderHandler(this);
        Map<String, String> headerMap = headerHandler.process(query.getInputStream(), this.getStreamDefinitionMap());


        setProcessingMode(ProcessingMode.INPUT_STREAM.SELECTOR);
        QuerySelectorProcessor querySelectorProcessor = new QuerySelectorProcessor(this);
        querySelectorProcessor.handleSelector(query.getSelector());
        ConcurrentMap<String, String> concurrentSelectorMap = querySelectorProcessor.getSelectorQueryMap();

        setProcessingMode(ProcessingMode.OUTPUT_STREAM);
        OutStream outStream = query.getOutputStream();
        StreamDefinitionExt outStreamDefinition = getStreamDefinition(outStream.getStreamId());
        TableCreatorBase tableCreator = new CSVTableCreator();
        tableCreator.setQuery(outStreamDefinition);
        String outputInsertQuery = tableCreator.getInsertQuery();
        String outputCreate = tableCreator.getQuery();

        Stream inStream = query.getInputStream();
        List<String> lstIDs = inStream.getStreamIds();
        StreamDefinitionExt inStreamDef;

        String [] arrCreate = new String[lstIDs.size()];
        int i = 0;
        for (String s : lstIDs) {
            inStreamDef = getStreamDefinition(s);
            tableCreator = new CassandraTableCreator();
            tableCreator.setQuery(inStreamDef);
            arrCreate[i++] = tableCreator.getQuery();
        }

        String inputCreate = "";
        for (int j=0; j<arrCreate.length; j++) {
            inputCreate += arrCreate[j];
            inputCreate += "\n";
        }


        String fromClause = headerMap.get(Constants.FROM_CLAUSE);
        if(fromClause == null)
            fromClause = headerMap.get(Constants.LENGTH_WIND_FROM_QUERY);
        if(fromClause == null)
            fromClause = headerMap.get(Constants.JOIN_CLAUSE);

        String selectQuery = "SELECT " + concurrentSelectorMap.get(Constants.SELECTION_QUERY);
        String groupByQuery = concurrentSelectorMap.get(Constants.GROUP_BY_QUERY);

        if(groupByQuery == null)
            groupByQuery = " ";

        String havingQuery = concurrentSelectorMap.get(Constants.HAVING_QUERY);

        if(havingQuery == null)
            havingQuery = " ";

        String whereClause = headerMap.get(Constants.WHERE_CLAUSE);

        if(whereClause == null)
            whereClause = " ";

        String incrementalClause = headerMap.get(Constants.INCREMENTAL_CLAUSE);

        if(incrementalClause == null)
            incrementalClause = " ";

       // hiveQuery = outputQuery + "\n" + incrementalClause + "\n" + fromClause + "\n " + selectQuery + "\n " + groupByQuery + "\n " + havingQuery + "\n " + whereClause + "\n ";
        hiveQuery = inputCreate + "\n" + outputCreate +"\n" +outputInsertQuery + "\n" + incrementalClause + "\n" + selectQuery + "\n " + fromClause + "\n " +whereClause + "\n " + groupByQuery + "\n " + havingQuery + "\n ";

        return hiveQuery;

    }
}
