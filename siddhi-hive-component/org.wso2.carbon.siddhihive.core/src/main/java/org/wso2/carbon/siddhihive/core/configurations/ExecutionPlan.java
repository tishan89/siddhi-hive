package org.wso2.carbon.siddhihive.core.configurations;


import java.util.Map;

public class ExecutionPlan {
    private String query;
    private Map<String, StreamDefinition> streamDefinitionMap;

    public ExecutionPlan(String query, Map<String, StreamDefinition> streamDefinitionMap) {
        this.query = query;
        this.streamDefinitionMap = streamDefinitionMap;
    }

    private ExecutionPlan() {
        //Do nothing
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Map<String, StreamDefinition> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public void setStreamDefinitionMap(Map<String, StreamDefinition> streamDefinitionMap) {
        this.streamDefinitionMap = streamDefinitionMap;
    }
}