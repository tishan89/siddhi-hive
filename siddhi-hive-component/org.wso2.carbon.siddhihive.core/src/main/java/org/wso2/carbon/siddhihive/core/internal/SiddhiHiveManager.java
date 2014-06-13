package org.wso2.carbon.siddhihive.core.internal;


import org.apache.log4j.Logger;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.headerprocessor.HeaderHandler;
import org.wso2.carbon.siddhihive.core.tablecreation.CSVTableCreator;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.carbon.siddhihive.core.tablecreation.CassandraTableCreator;
import org.wso2.carbon.siddhihive.core.tablecreation.TableCreatorBase;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
Class to manage query conversion in higher level. Will call appropriate handlers. Will also contain initial data needed for the conversion.
 */
public class SiddhiHiveManager {


    private static final Logger log = Logger.getLogger(SiddhiHiveManager.class);
    private Map<String, StreamDefinitionExt> streamDefinitionMap; //contains stream definition
    private Map<String, String> queryMap;

    public SiddhiHiveManager() {
        streamDefinitionMap = new ConcurrentHashMap<String, StreamDefinitionExt>();
        //New Query Map
        queryMap = new ConcurrentHashMap<String, String>();
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
        TableCreatorBase tableCreator = new CassandraTableCreator();
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
