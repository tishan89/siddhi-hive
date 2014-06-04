package org.wso2.carbon.siddhihive.core;


import org.wso2.carbon.siddhihive.core.headerprocessor.WindowStreamHandler;
import org.wso2.carbon.siddhihive.querygenerator.HiveTableCreator;
import org.wso2.carbon.siddhihive.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
Class to manage query conversion in higher level. Will call appropriate handlers. Will also contain initial data needed for the conversion.
 */
public class SiddhiHiveManager {

    private static SiddhiHiveManager siddhiHiveManager = null;

    private ConcurrentMap<String, StreamDefinition> streamDefinitionMap; //contains stream definition
    private ConcurrentMap<String, String> queryMap;

    private SiddhiHiveManager(/*StreamDefinition streamDefinition*/) {

        //siddhiHiveManager = null;

        streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinition>();
        //streamDefinitionMap.put(streamDefinition.getStreamId(), streamDefinition);

        //New Query Map
        queryMap = new ConcurrentHashMap<String, String>();
    }

    public static SiddhiHiveManager getInstance() {

        if (siddhiHiveManager == null)
            siddhiHiveManager = new SiddhiHiveManager();

        return siddhiHiveManager;
    }

    public ConcurrentMap<String, StreamDefinition> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setStreamDefinitionMap(ConcurrentMap<String, StreamDefinition> streamDefinitionMap) {
        this.streamDefinitionMap = streamDefinitionMap;
    }

    public void setStreamDefinition(String streamDefinitionID, StreamDefinition streamDefinition) {
        streamDefinitionMap.put(streamDefinitionID, streamDefinition);
    }

    public StreamDefinition getStreamDefinition(String streamId) {
        return streamDefinitionMap.get(streamId);
    }

    public String getQuery(Query query) {


        String hiveQuery = "";
        WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
        //WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
        Map<String, String> windowStreamMap = windowStreamHandler.process(query.getInputStream());

        QuerySelectorProcessor querySelectorProcessor = new QuerySelectorProcessor();
        querySelectorProcessor.handleSelector(query.getSelector());
        ConcurrentMap<String, String> concurrentSelectorMap = querySelectorProcessor.getSelectorQueryMap();

        OutStream outStream = query.getOutputStream();
        StreamDefinition outStreamDefinition = getStreamDefinition(outStream.getStreamId());

        HiveTableCreator hiveTableCreator = new HiveTableCreator();
        String outputQuery = hiveTableCreator.getQuery(outStreamDefinition);

        //hiveQuery = outputQuery + "\n" +


        String fromClause = windowStreamMap.get(Constants.FROM_CLAUSE);
        String selectQuery = "SELECT " + concurrentSelectorMap.get(Constants.SELECTION_QUERY);
        String groupByQuery = concurrentSelectorMap.get(Constants.GROUP_BY_QUERY);
        String havingQuery = concurrentSelectorMap.get(Constants.HAVING_QUERY);

        String whereClause = windowStreamMap.get(Constants.WHERE_CLAUSE);
        String incrementalClause = windowStreamMap.get(Constants.INCREMENTAL_CLAUSE);

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
