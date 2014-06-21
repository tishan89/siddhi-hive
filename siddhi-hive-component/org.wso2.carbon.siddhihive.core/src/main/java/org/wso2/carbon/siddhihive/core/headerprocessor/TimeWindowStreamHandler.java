package org.wso2.carbon.siddhihive.core.headerprocessor;


import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.WindowStreamProcessingLevel;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Window;

import java.util.HashMap;
import java.util.Map;

public class TimeWindowStreamHandler extends WindowStreamHandler {
    private String windowIsolatorClause;
    private String fromClause;
    private String whereClause;
    private String schedulingFreq;
    private WindowStream windowStream;
    private WindowIsolator windowIsolator;
    private Map<String, String> result;

    public TimeWindowStreamHandler() {
        this.windowIsolator = new WindowIsolator();
    }

    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        this.windowStream = (WindowStream) stream;

        //addStreamReference(this.windowStream.getStreamReferenceId(), this.windowStream.getStreamId());

        windowIsolatorClause = generateIsolatorClause(windowStream.getStreamId(), windowStream.getWindow(), streamDefinitions);
        fromClause = generateFromClause(windowStream.getStreamId());
        whereClause = generateWhereClause(windowStream.getFilter());
        schedulingFreq = generateSchedulingFrequency(windowStream.getWindow());
        result = new HashMap<String, String>();
        result.put(Constants.FROM_CLAUSE, fromClause);
        result.put(Constants.WHERE_CLAUSE, whereClause);
        result.put(Constants.INCREMENTAL_CLAUSE, windowIsolatorClause);
        result.put(Constants.TIME_WINDOW_FREQUENCY, schedulingFreq);

        Context context = StateManager.getContext();
        context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.TIME_WINDOW_PROCESSING);
        StateManager.setContext(context);

        return result;
    }

    private String generateSchedulingFrequency(Window window) {
        if (window.getName().equals(Constants.TIME_WINDOW)) {
            return Constants.DEFAULT_SLIDING_FREQUENCY;
        } else if (window.getName().equals(Constants.TIME_BATCH_WINDOW)) {
            if (window.getParameters()[0] instanceof LongConstant) {
                return String.valueOf(((LongConstant) window.getParameters()[0]).getValue());
            } else if (window.getParameters()[0] instanceof IntConstant) {
                return String.valueOf(((IntConstant) window.getParameters()[0]).getValue());
            }
        }
        return null;
    }

    private String generateIsolatorClause(String streamId, Window window, Map<String, StreamDefinitionExt> streamDefinitions) {
        StreamDefinitionExt streamDefinitionExt = streamDefinitions.get(streamId);
        return windowIsolator.process(windowStream.getWindow(), streamDefinitionExt);
    }
}
