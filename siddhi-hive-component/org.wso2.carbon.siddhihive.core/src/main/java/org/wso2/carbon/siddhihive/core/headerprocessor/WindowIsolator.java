package org.wso2.carbon.siddhihive.core.headerprocessor;


import org.wso2.carbon.siddhihive.core.configurations.StreamDefinition;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.query.input.handler.Window;

import java.util.HashMap;
import java.util.Map;

public class WindowIsolator {

    private String type;
    private Map<String, String> propertyMap;
    private String isolatorClause;

    public WindowIsolator() {
        propertyMap = new HashMap<String, String>();
    }

    public String process(Window window, StreamDefinition streamDefinition) {

        this.type = window.getName();
        if (type.equals(Constants.TIME_WINDOW)) {
            this.populateForTimeWindow(window);
            isolatorClause = this.generateIsolateClause(streamDefinition);
        }

        return isolatorClause;
    }

    private String generateIsolateClause(org.wso2.carbon.siddhihive.core.configurations.StreamDefinition streamDefinition) {//Added this method for future configurations
        int bufferTime = 0;
        String name = streamDefinition.getStreamDefinition().getStreamId() + System.currentTimeMillis();
        return getIncrementalClause(name, streamDefinition.getStreamDefinition().getStreamId(), true, bufferTime);
    }

    public String getIncrementalClause(String name, String table, Boolean dataindexing, int bufferTime) {
        String generalClause = Constants.INCREMENTAL_KEYWORD + "(" + Constants.NAME + "=\"" + name + "\", " + Constants.TABLE_REFERENCE + "=\"" + table + "\", " + Constants.HAS_NON_INDEX_DATA + "=\"" + dataindexing + "\", " + Constants.BUFFER_TIME + "=\"" + bufferTime + "\"";
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(generalClause);
        for (Map.Entry<String, String> entry : propertyMap.entrySet()) {
            stringBuffer.append(", " + entry.getKey() + "=\"" + entry.getValue() + "\"");

        }
        stringBuffer.append(")");
        return stringBuffer.toString();
    }

    private void populateForTimeWindow(Window window) {
        propertyMap = new HashMap<String, String>();
        long currentTime = System.currentTimeMillis();
        long duration = (long) ((IntConstant) window.getParameters()[0]).getValue();
        long toTime = currentTime + duration;
        propertyMap.put(Constants.FROM_TIME, String.valueOf(currentTime));
        propertyMap.put(Constants.TO_TIME, String.valueOf(toTime));
    }
}
