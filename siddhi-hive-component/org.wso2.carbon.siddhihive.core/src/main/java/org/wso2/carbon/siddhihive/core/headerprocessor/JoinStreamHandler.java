package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.siddhi.query.api.query.input.Stream;

import java.util.Map;

/**
 * Created by root on 6/3/14.
 */
public class JoinStreamHandler implements StreamHandler {

    public JoinStreamHandler() {

    }


    @Override
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions) {
        return null;
    }
}
