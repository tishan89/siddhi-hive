package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.JoinStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 6/3/14.
 */
public class JoinStreamHandler implements StreamHandler {
    //**********************************************************************************************
    private Map<String, String> result;
    private JoinStream joinStream;

    //**********************************************************************************************
    public JoinStreamHandler() {

    }

    //**********************************************************************************************
    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        joinStream = (JoinStream)stream;
        Map<String, String> mapLeftStream = processSubStream(joinStream.getLeftStream(), streamDefinitions);
        Map<String, String> mapRightStream = processSubStream(joinStream.getRightStream(), streamDefinitions);

        ConditionHandler conditionHandler = new ConditionHandler();
        String sCondition = conditionHandler.processCondition(joinStream.getOnCompare());

        result = new HashMap<String, String>();
        return result;
    }

    //**********************************************************************************************
    private Map<String, String> processSubStream(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        Map<String, String> result;
        if (stream instanceof BasicStream) {
            BasicStreamHandler basicStreamHandler = new BasicStreamHandler();
            result = basicStreamHandler.process(stream, streamDefinitions);
        } else if (stream instanceof WindowStream) {
            WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
            result = windowStreamHandler.process(stream, streamDefinitions);
        } else {
            result = null;
        }
        return result;
    }
}
