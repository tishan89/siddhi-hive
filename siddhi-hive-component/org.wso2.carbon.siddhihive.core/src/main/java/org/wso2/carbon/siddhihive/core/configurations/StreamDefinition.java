package org.wso2.carbon.siddhihive.core.configurations;


public class StreamDefinition {
    private String fullQualifiedStreamID;
    private org.wso2.siddhi.query.api.definition.StreamDefinition streamDefinition;

    public StreamDefinition(String streamId, org.wso2.siddhi.query.api.definition.StreamDefinition definition) {
        this.fullQualifiedStreamID = streamId;
        this.streamDefinition = definition;
    }

    public String getFullQualifiedStreamID() {
        return fullQualifiedStreamID;
    }

    public void setFullQualifiedStreamID(String fullQualifiedStreamID) {
        this.fullQualifiedStreamID = fullQualifiedStreamID;
    }

    public org.wso2.siddhi.query.api.definition.StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public void setStreamDefinition(org.wso2.siddhi.query.api.definition.StreamDefinition streamDefinition) {
        this.streamDefinition = streamDefinition;
    }
}
