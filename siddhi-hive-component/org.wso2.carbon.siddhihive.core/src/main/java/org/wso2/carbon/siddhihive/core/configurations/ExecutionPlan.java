package org.wso2.carbon.siddhihive.core.configurations;


import java.util.Map;

public class ExecutionPlan {
    private String query;
    private Map<String, StreamDefinition> streamDefinitionMap;

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
