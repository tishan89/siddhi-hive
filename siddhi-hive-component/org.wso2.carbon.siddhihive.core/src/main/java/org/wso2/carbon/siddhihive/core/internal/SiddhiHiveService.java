package org.wso2.carbon.siddhihive.core.internal;


import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.SiddhiHiveServiceInterface;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.query.Query;

import java.util.Map;

public class SiddhiHiveService implements SiddhiHiveServiceInterface {
    private SiddhiHiveManager siddhiHiveManager;
    private SiddhiManager siddhiManager;

    @Override
    public String addExecutionPlan(ExecutionPlan executionPlan) {
        siddhiHiveManager = new SiddhiHiveManager();
        siddhiHiveManager.setStreamDefinitionMap(executionPlan.getStreamDefinitionMap());
        configureSiddhiManager(executionPlan.getStreamDefinitionMap());
        String queryID = siddhiManager.addQuery(executionPlan.getQuery());
        siddhiHiveManager.setSiddhiStreamDefinition(siddhiManager.getStreamDefinitions());
        Query query = siddhiManager.getQuery(queryID);
        String hiveQueryString = siddhiHiveManager.getQuery(query);
        return hiveQueryString;
    }

    private void configureSiddhiManager(Map<String, StreamDefinitionExt> streamDefinitions) {
        siddhiManager = new SiddhiManager();
        for (Map.Entry<String, StreamDefinitionExt> entry : streamDefinitions.entrySet()) {
            siddhiManager.defineStream(entry.getValue().getStreamDefinition());

        }
    }



    /*@Override
    public Boolean addStreamDefinition(StreamDefinitionExt streamDefinition, String fullQualifiedname) {
        return null;
    }*/
}
