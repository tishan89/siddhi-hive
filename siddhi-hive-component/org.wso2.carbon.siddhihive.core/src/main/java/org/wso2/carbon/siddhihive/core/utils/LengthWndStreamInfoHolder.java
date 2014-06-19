package org.wso2.carbon.siddhihive.core.utils;

import org.wso2.carbon.siddhihive.core.headerprocessor.LengthWindowStreamHandler;

/**
 * Created by firzhan on 6/18/14.
 */
public class LengthWndStreamInfoHolder {

    private String streamReferenceID = null;
    private String streamID = null;
    private String subQueryIdentifier = null;

    public LengthWndStreamInfoHolder(){

    }

    public String getSubQueryIdentifier() {
        return subQueryIdentifier;
    }

    public void setSubQueryIdentifier(String subQueryIdentifier) {
        this.subQueryIdentifier = subQueryIdentifier;
    }

    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public String getStreamReferenceID() {
        return streamReferenceID;
    }

    public void setStreamReferenceID(String streamReferenceID) {
        this.streamReferenceID = streamReferenceID;
    }
}
