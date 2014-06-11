package org.wso2.carbon.siddhihive.core.internal;


import org.apache.log4j.Logger;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
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
    private ConcurrentMap<String, StreamDefinitionExt> streamDefinitionMap; //contains stream definition
    private ConcurrentMap<String, String> queryMap;

    public SiddhiHiveManager() {
        streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinitionExt>();
        //New Query Map
        queryMap = new ConcurrentHashMap<String, String>();
    }

    public ConcurrentMap<String, StreamDefinitionExt> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setStreamDefinitionMap(ConcurrentMap<String, StreamDefinitionExt> streamDefinitionMap) {
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
        HeaderHandler headerHandler = new HeaderHandler();
        Map<String, String> headerMap = headerHandler.process(query.getInputStream(), this.getStreamDefinitionMap());


        QuerySelectorProcessor querySelectorProcessor = new QuerySelectorProcessor();
        querySelectorProcessor.handleSelector(query.getSelector());
        ConcurrentMap<String, String> concurrentSelectorMap = querySelectorProcessor.getSelectorQueryMap();

        OutStream outStream = query.getOutputStream();
        StreamDefinitionExt outStreamDefinition = getStreamDefinition(outStream.getStreamId());



        CSVTableCreator CSVTableCreator = new CSVTableCreator();
        CSVTableCreator.setQuery(outStreamDefinition);

        String insertQuery = CSVTableCreator.getInsertQuery();
        String createQuery = CSVTableCreator.getQuery();
        //hiveQuery = outputQuery + "\n" +


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
        hiveQuery = createQuery +"\n" +insertQuery + "\n" + incrementalClause + "\n" + selectQuery + "\n " + fromClause + "\n " +whereClause + "\n " + groupByQuery + "\n " + havingQuery + "\n ";

        return hiveQuery;

    }
}
