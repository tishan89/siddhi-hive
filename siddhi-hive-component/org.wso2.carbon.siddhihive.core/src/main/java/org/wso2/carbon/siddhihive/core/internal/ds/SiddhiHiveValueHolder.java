package org.wso2.carbon.siddhihive.core.internal.ds;


import org.wso2.carbon.event.stream.manager.core.EventStreamService;

import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

public class SiddhiHiveValueHolder {
    private EventStreamService eventStreamService;
    private static SiddhiHiveValueHolder siddhiHiveValueHolder;
    private SiddhiHiveService siddhiHiveService;

    private SiddhiHiveValueHolder() {
        //Do nothing
    }

    public static SiddhiHiveValueHolder getInstance() {
        if (siddhiHiveValueHolder == null) {
            siddhiHiveValueHolder = new SiddhiHiveValueHolder();
        }
        return siddhiHiveValueHolder;
    }

    public EventStreamService getEventStreamService() {
        return eventStreamService;
    }

    public void setEventStreamService(EventStreamService eventStreamService) {
        this.eventStreamService = eventStreamService;
    }

    public void unsetEventStreamService() {
        this.eventStreamService = null;
    }

    public void registerSiddhiHiveService(SiddhiHiveService siddhiHiveService) {
        this.siddhiHiveService = siddhiHiveService;
    }

    public SiddhiHiveService getSiddhiHiveService() {
        return siddhiHiveService;
    }
}
