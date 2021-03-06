package org.wso2.carbon.siddhihive.core.headerprocessor;


import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.JoinStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;

import java.util.Map;

public class HeaderHandler implements StreamHandler {

    SiddhiHiveManager siddhiHiveManager = null;

    public HeaderHandler(SiddhiHiveManager siddhiHiveManager){
       this.siddhiHiveManager = siddhiHiveManager;
    }

    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        Map<String, String> result;
        if (stream instanceof BasicStream) {
            BasicStreamHandler basicStreamHandler = new BasicStreamHandler();
            result = basicStreamHandler.process(stream, streamDefinitions);
        } else if (stream instanceof WindowStream) {
            WindowStreamHandler windowStreamHandler = new WindowStreamHandler();
            result = windowStreamHandler.process(stream, streamDefinitions);
        } else if (stream instanceof JoinStream) {
            JoinStreamHandler joinStreamHandler = new JoinStreamHandler();
            result = joinStreamHandler.process(stream, streamDefinitions);
        } else {
            result = null;
        }
        return result;
    }
}
