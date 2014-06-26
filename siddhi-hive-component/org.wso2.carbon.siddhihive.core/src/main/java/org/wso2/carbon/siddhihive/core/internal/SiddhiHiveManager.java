package org.wso2.carbon.siddhihive.core.internal;


import org.apache.log4j.Logger;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.stream.manager.core.exception.EventStreamConfigurationException;
import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.headerprocessor.HeaderHandler;
import org.wso2.carbon.siddhihive.core.internal.ds.SiddhiHiveValueHolder;
import org.wso2.carbon.siddhihive.core.tablecreation.CSVTableCreator;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.tablecreation.CassandraTableCreator;
import org.wso2.carbon.siddhihive.core.tablecreation.TableCreatorBase;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.SiddhiHiveToolBoxCreator;
import org.wso2.carbon.siddhihive.core.utils.enums.*;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

import java.util.*;
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

    //private Map<String, String> inputStreamGeneratedQueryMap = null; // reference ID <-----> Replacement generatedQueryID
   // private Map<String, String> cachedValuesMap= null; //parent refernce
   // private Map<String, String> inputStreamGeneratedQueryMap= null; // reference ID <-----> Replacement generatedQueryID

//    private ProcessingLevel processingLevel;
//    private InputStreamProcessingLevel inputStreamProcessingLevel;
//    private SelectorProcessingLevel selectorProcessingLevel;
//    private WindowStreamProcessingLevel windowStreamProcessingLevel;
//    private WindowProcessingLevel windowProcessingLevel;
//
//    private int subQueryCounter = 0;
//
//    private Map<String, String> selectionAttributeRenameMap = null;
//
//    private Map<String, String> referenceIDAliasMap = null;
    private Boolean isScheduled = false;
    private Query query;


    public SiddhiHiveManager() {
        streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinitionExt>();
        //New Query Map
        // queryMap = new ConcurrentHashMap<String, String>();
        //inputStreamReferenceIDMap = new ConcurrentHashMap<String, String>();
//        cachedValuesMap = new ConcurrentHashMap<String, String>();
//        inputStreamGeneratedQueryMap = new ConcurrentHashMap<String, String>();
//        selectionAttributeRenameMap = new ConcurrentHashMap<String, String>();
//        referenceIDAliasMap = new ConcurrentHashMap<String, String>();

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

    public void setSiddhiStreamDefinition(List<StreamDefinition> streamDefinitionList) {

        for (StreamDefinition definition : streamDefinitionList) {
            int i = 0;
            for (Map.Entry<String, StreamDefinitionExt> entry : streamDefinitionMap.entrySet()) {
                i++;
                if ((!(definition.getStreamId().equals(entry.getKey()))) && (i < streamDefinitionMap.entrySet().size())) {
                    continue;

                } else if (definition.getStreamId().equals(entry.getKey())) {
                    break;
                }
                StreamDefinitionExt streamDefinition = new StreamDefinitionExt(definition.getStreamId(), definition);
                this.setStreamDefinition(streamDefinition.getFullQualifiedStreamID(), streamDefinition);
            }
        }
    }

    public StreamDefinitionExt getStreamDefinition(String streamId) {
        return streamDefinitionMap.get(streamId);
    }

    public String getQuery(Query query) {

        Boolean incrementalEnabled = false;
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


        String fromClause = headerMap.get(Constants.FROM_CLAUSE);
        if (fromClause == null)
            fromClause = headerMap.get(Constants.LENGTH_WIND_FROM_QUERY);
        if(fromClause == null)
            fromClause = headerMap.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY);

        if (fromClause == null)
            fromClause = headerMap.get(Constants.JOIN_CLAUSE);

        String initializationScript = headerMap.get(Constants.INITALIZATION_SCRIPT);

        if (initializationScript == null)
            initializationScript = " ";

        String selectQuery = "SELECT \'Dummy Key\', " + concurrentSelectorMap.get(Constants.SELECTION_QUERY);
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

        if (incrementalClause == null) {
            incrementalClause = " ";
        } else {
            incrementalEnabled = true;
        }

       // hiveQuery = outputQuery + "\n" + incrementalClause + "\n" + fromClause + "\n " + selectQuery + "\n " + groupByQuery + "\n " + havingQuery + "\n " + whereClause + "\n ";
        hiveQuery = initializationScript  +  inputCreate + "\n" + outputCreate +"\n" + "\n" +  Constants.INITIALIZATION_STATEMENT + "\n" + incrementalClause + "\n" + outputInsertQuery + "\n" + selectQuery + "\n " + fromClause + "\n " +whereClause + "\n " + groupByQuery + "\n " + havingQuery + "\n " +";";
        List<String> streamDefs = new ArrayList<String>();
        for (Map.Entry entry : streamDefinitionMap.entrySet()) {
            if (isInputStream((StreamDefinitionExt) entry.getValue())) {
                streamDefs.add(getOriginalStreamDefinition((StreamDefinitionExt) entry.getValue()));
            }
        }
        SiddhiHiveToolBoxCreator siddhiHiveToolBoxCreator = new SiddhiHiveToolBoxCreator(streamDefs, hiveQuery);
        Long freq = getSlidingFreq(headerMap.get(Constants.TIME_WINDOW_FREQUENCY), headerMap.get(Constants.TIME_BATCH_WINDOW_FREQUENCY), headerMap.get(Constants.LENGTH_WINDOW_FREQUENCY), headerMap.get(Constants.LENGTH_WINDOW_BATCH_FREQUENCY));
        siddhiHiveToolBoxCreator.createToolBox(incrementalEnabled, freq);
        context.reset();
        StateManager.setContext(context);

        return hiveQuery;

    }

    private Long getSlidingFreq(String timeWindow, String timeBatchWindow, String lengthWindow, String lengthBatchWindow) {
        List<Long> freq = new ArrayList<Long>();
        if (timeWindow != null) {
            freq.add(Long.parseLong(timeWindow));
        }
        if (timeBatchWindow != null) {
            freq.add(Long.parseLong(timeBatchWindow));
        }
        if (lengthBatchWindow != null) {
            freq.add(Long.parseLong(lengthBatchWindow));
        }
        if (lengthWindow != null) {
            freq.add(Long.parseLong(lengthWindow));
        }
        if (freq.size() > 0) {
            Collections.sort(freq);
            return freq.get(0);
        } else {
            return Long.valueOf(0);
        }
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

    private long getScheduleTime(Map<String, String> timeMap, String frequency, String batchFrequency){

        String scheduleTime = null;
        String batchScheduleTime = null;

        scheduleTime = timeMap.get(frequency);
        batchScheduleTime = timeMap.get(batchFrequency);

        long scheduleValue = 0l;
        long batchScheduleValue = 0l;

        long scheduleFinalValue = 0l;

        if( (scheduleTime != null) && (batchScheduleTime != null) ){

            scheduleValue = Long.valueOf(scheduleTime).longValue();
            batchScheduleValue = Long.valueOf(batchScheduleTime).longValue();

            if(scheduleValue > batchScheduleValue)
                scheduleFinalValue = batchScheduleValue;
            else
                scheduleFinalValue = scheduleValue;
        }else if(scheduleTime != null){
            scheduleFinalValue =  Long.valueOf(scheduleTime).longValue();
        }else if(batchScheduleTime != null){
            scheduleFinalValue =  Long.valueOf(batchScheduleTime).longValue();
        }

        return scheduleFinalValue;
    }

    private Boolean isInputStream(StreamDefinitionExt streamDefinitionExt) {
        if (streamDefinitionExt.getFullQualifiedStreamID().equals(streamDefinitionExt.getStreamDefinition().getStreamId())) {
            return false;
        } else {
            return true;
        }
    }

    private String getOriginalStreamDefinition(StreamDefinitionExt streamDefinitionExt) {
        int tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
        String streamId = streamDefinitionExt.getFullQualifiedStreamID();
        org.wso2.carbon.databridge.commons.StreamDefinition streamDefinition = null;
        try {
            streamDefinition = SiddhiHiveValueHolder.getInstance().getEventStreamService().getStreamDefinition(streamId, tenantId);
        } catch (EventStreamConfigurationException e) {
            e.printStackTrace();
        }
        if (streamDefinition != null) {
            return streamDefinition.toString();
        } else {
            log.error("No stream definition found for stream id " + streamId);
            return null;
        }
    }


}
