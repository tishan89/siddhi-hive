package org.wso2.carbon.siddhihive.core.headerprocessor;


import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import java.util.Map;

public class WindowStreamHandler extends BasicStreamHandler {

    private WindowStream windowStream;
    private Object type;

    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        windowStream = (WindowStream) stream;
        this.type = windowStream.getWindow().getName();
        if (type.equals(Constants.TIME_WINDOW) || type.equals(Constants.TIME_BATCH_WINDOW)) {
            TimeWindowStreamHandler timeWindowStreamHandler = new TimeWindowStreamHandler();
            return timeWindowStreamHandler.process(windowStream, streamDefinitions);
        } else if (type.equals(Constants.LENGTH_WINDOW) || type.equals(Constants.LENGTH_BATCH_WINDOW)) {
            return null; //TODO
        }
        return null;
    }


}
