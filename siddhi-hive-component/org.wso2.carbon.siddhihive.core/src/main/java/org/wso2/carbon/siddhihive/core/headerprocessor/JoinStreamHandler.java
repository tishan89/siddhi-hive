package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.Conversions;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.JoinStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.compiler.SiddhiQLGrammarParser;

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

        String sJoin = Conversions.siddhiToHiveJoin(joinStream.getType());

        String sLeftString = mapLeftStream.get(Constants.FROM_CLAUSE);
        if (sLeftString == null)
            sLeftString = mapLeftStream.get(Constants.LENGTH_WIND_FROM_QUERY);
        sLeftString = sLeftString.replaceFirst(Constants.FROM+" ", "");

        String sRightString = mapRightStream.get(Constants.FROM_CLAUSE);
        if (sRightString == null)
            sRightString = mapRightStream.get(Constants.LENGTH_WIND_FROM_QUERY);
        sRightString = sRightString.replaceFirst(Constants.FROM+" ", "");

        String sQuery = "from (select * from " + sLeftString + " "+ sJoin + " " + sRightString+ " ON " + sCondition + ")";

        result = new HashMap<String, String>();
        result.put(Constants.JOIN_CLAUSE, sQuery);
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
