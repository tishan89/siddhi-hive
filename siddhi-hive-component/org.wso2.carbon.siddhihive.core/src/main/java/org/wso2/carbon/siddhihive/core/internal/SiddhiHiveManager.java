package org.wso2.carbon.siddhihive.core.internal;


import org.apache.log4j.Logger;
import org.wso2.carbon.siddhihive.core.headerprocessor.HeaderHandler;
import org.wso2.carbon.siddhihive.core.tablecreation.CSVTableCreator;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
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
    private ConcurrentMap<String, org.wso2.carbon.siddhihive.core.configurations.StreamDefinition> streamDefinitionMap; //contains stream definition
    private ConcurrentMap<String, String> queryMap;

    public SiddhiHiveManager() {
        streamDefinitionMap = new ConcurrentHashMap<String, org.wso2.carbon.siddhihive.core.configurations.StreamDefinition>();
        //New Query Map
        queryMap = new ConcurrentHashMap<String, String>();
    }

    public ConcurrentMap<String, org.wso2.carbon.siddhihive.core.configurations.StreamDefinition> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setStreamDefinitionMap(ConcurrentMap<String, org.wso2.carbon.siddhihive.core.configurations.StreamDefinition> streamDefinitionMap) {
        this.streamDefinitionMap = streamDefinitionMap;
    }

    public void setStreamDefinition(String streamDefinitionID, org.wso2.carbon.siddhihive.core.configurations.StreamDefinition streamDefinition) {
        streamDefinitionMap.put(streamDefinitionID, streamDefinition);
    }

    public void setStreamDefinition(List<org.wso2.carbon.siddhihive.core.configurations.StreamDefinition> streamDefinitionList) {
        for (org.wso2.carbon.siddhihive.core.configurations.StreamDefinition definition : streamDefinitionList) {
            streamDefinitionMap.put(definition.getStreamDefinition().getStreamId(), definition);
        }
    }

    public void setSiddhiStreamDefinition(List<StreamDefinition> streamDefinitionList) {
        for (StreamDefinition definition : streamDefinitionList) {
            for (Map.Entry<String, org.wso2.carbon.siddhihive.core.configurations.StreamDefinition> entry : streamDefinitionMap.entrySet()) {
                if (!(definition.getStreamId().equals(entry.getKey()))) {
                    org.wso2.carbon.siddhihive.core.configurations.StreamDefinition streamDefinition = new org.wso2.carbon.siddhihive.core.configurations.StreamDefinition(definition.getStreamId(), definition);
                    this.setStreamDefinition(streamDefinition.getFullQualifiedStreamID(), streamDefinition);
                }
            }
        }
    }

    public org.wso2.carbon.siddhihive.core.configurations.StreamDefinition getStreamDefinition(String streamId) {
        return streamDefinitionMap.get(streamId);
    }

    public String getQuery(Query query) {


        String hiveQuery = "";
        HeaderHandler headerHandler = new HeaderHandler();
        Map<String, String> headerMap = headerHandler.process(query.getInputStream(), this.getStreamDefinitionMap());


        QuerySelectorProcessor querySelectorProcessor = new QuerySelectorProcessor();
        querySelectorProcessor.handleSelector(query.getSelector());
        ConcurrentMap<String, String> concurrentSelectorMap = querySelectorProcessor.getSelectorQueryMap();

        OutStream outStream = query.getOutputStream();
        org.wso2.carbon.siddhihive.core.configurations.StreamDefinition outStreamDefinition = getStreamDefinition(outStream.getStreamId());

        CSVTableCreator CSVTableCreator = new CSVTableCreator();
        //CSVTableCreator.setQuery(outStreamDefinition);

        String outputQuery = CSVTableCreator.getInsertQuery();
        //hiveQuery = outputQuery + "\n" +


        String fromClause = headerMap.get(Constants.FROM_CLAUSE);
        String selectQuery = "SELECT " + concurrentSelectorMap.get(Constants.SELECTION_QUERY);
        String groupByQuery = concurrentSelectorMap.get(Constants.GROUP_BY_QUERY);
        String havingQuery = concurrentSelectorMap.get(Constants.HAVING_QUERY);

        String whereClause = headerMap.get(Constants.WHERE_CLAUSE);
        String incrementalClause = headerMap.get(Constants.INCREMENTAL_CLAUSE);

        hiveQuery = outputQuery + "\n" + incrementalClause + "\n" + fromClause + "\n " + selectQuery + "\n " + groupByQuery + "\n " + havingQuery + "\n " + whereClause + "\n ";

        return hiveQuery;

    }


//    public String getHiveQuery(Query query){
//
//        if(query == null)
//            return null;
//
//        String hiveQuery = "";
//
//
//
//        WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
//        HashMap<String, String> windowStreamMap = windowStreamHandler.process()
//
//
//
//    }
//
//    private String handleStream(Stream stream){
//
//        String resultQuery = "";
//
//        if(stream instanceof WindowStream){
//
//        }
//
////        if(stream instanceof BasicStream){
////
////        }
////        else if(stream instanceof JoinStream){
////
////            JoinStream joinStream = (JoinStream) stream;
////
////            String leftStream = handleStream(joinStream.getLeftStream());
////            String rightStream = handleStream(joinStream.getRightStream());
////        }
////        else if(stream instanceof WindowStream){
////
////        }
//
//    }

//    private Stream handleJoinStream(JoinStream joinStream){
//
//
//    }


}
