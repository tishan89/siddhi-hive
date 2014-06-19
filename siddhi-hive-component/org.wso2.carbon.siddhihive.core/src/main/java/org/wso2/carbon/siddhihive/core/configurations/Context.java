package org.wso2.carbon.siddhihive.core.configurations;

/*
Class to hold context information needed in processing
queries.
 */
public class Context {


    public Boolean getIsScheduled() {
        return isScheduled;
    }

    public void setIsScheduled(Boolean isScheduled) {
        this.isScheduled = isScheduled;
    }

    private Boolean isScheduled = false;


}
