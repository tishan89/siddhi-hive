package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.Conversions;
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

        String sJoin = Conversions.siddhiToHiveJoin(joinStream.getType());

        String sLeftString = mapLeftStream.get(Constants.FROM_CLAUSE);
        if (sLeftString == null)
            sLeftString = mapLeftStream.get(Constants.LENGTH_WIND_FROM_QUERY);
        if(sLeftString == null)
            sLeftString = mapLeftStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY);

        sLeftString = sLeftString.replaceFirst(Constants.FROM+" ", "");

        String sRightString = mapRightStream.get(Constants.FROM_CLAUSE);
        if (sRightString == null)
            sRightString = mapRightStream.get(Constants.LENGTH_WIND_FROM_QUERY);
        if(sRightString == null)
            sRightString = mapRightStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY);

        sRightString = sRightString.replaceFirst(Constants.FROM+" ", "");

        String aliasID = "";

        Context context = StateManager.getContext();
        if(joinStream.getLeftStream() instanceof WindowStream){

            WindowStream windowStream = (WindowStream) joinStream.getLeftStream();
            aliasID =  context.generateSubQueryIdentifier();
            context.setReferenceIDAlias(windowStream.getStreamReferenceId(), aliasID);
           // StateManager.setContext(context);

        }

        if(joinStream.getRightStream() instanceof WindowStream){
            WindowStream windowStream = (WindowStream) joinStream.getRightStream();

            if(aliasID.isEmpty() == true){
                aliasID =  context.generateSubQueryIdentifier();
            }
            context.setReferenceIDAlias(windowStream.getStreamReferenceId(), aliasID);
            //StateManager.setContext(context);
        }

        String appendingLeftSelectPhrase = "select * from";
        String appendingRightSelectPhrase = " ";

        if(mapLeftStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY) != null){
            appendingLeftSelectPhrase = mapLeftStream.get(Constants.FUNCTION_JOIN_LEFT_CALL_PARAM);
            appendingLeftSelectPhrase = "select *, " + appendingLeftSelectPhrase + "  from";

        }



        if(mapRightStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY) != null){
            appendingRightSelectPhrase = mapRightStream.get(Constants.FUNCTION_JOIN_RIGHT_CALL_PARAM);

            int count = 0;
            for(int i =0; i < sRightString.length(); i++){
                if(sRightString.charAt(i) != '*')
                    count++;
                else
                    break;
            }

            //int starStringIndex = sRightString.indexOf("\\*");

            String s1 = sRightString.substring(0,count+1);
            String s2 = sRightString.substring(count+1,sRightString.length());

            sRightString =  s1 + " , " + appendingRightSelectPhrase + " " + " as ABC " + s2;

//            String[] rightStreamSplit = sRightString.split("\\*");
//            rightStreamSplit[0] = rightStreamSplit[0] + appendingRightSelectPhrase + " " + " as ABC ";
//
//            sRightString = " ";
//
//            for(int i = 0; i < rightStreamSplit.length; i++){
//                sRightString += rightStreamSplit[i];
//            }

        }

//        if(mapRightStream.get(Constants.LENGTH_WIND_FROM_QUERY) != null){
//            appendingLeftSelectPhrase = mapRightStream.get(Constants.FUNCTION_JOIN_LEFT_CALL_PARAM);
//            appendingLeftSelectPhrase = " (select *, " + appendingLeftSelectPhrase + "  from  (    ";
//            String  rightStreamAlias = mapRightStream.get("ALIAS");
//
//            if(rightStreamAlias != null){
//                rightStreamAlias = " )" + rightStreamAlias;
//            }
//
//            sRightString = appendingLeftSelectPhrase + sRightString + " ) "+ rightStreamAlias;
//        }
        //    if(mapRightStream.get(Constants.LENGTH_BATCH_WIND_FROM_QUERY) != null)

        String sQuery = "from (  " + appendingLeftSelectPhrase + " " + sLeftString + " "+ sJoin + " " + sRightString+ " ON   (" + sCondition + ")" + " ) "+  aliasID;
        //String sQuery = "from (select * from " + sLeftString + " "+ sJoin + " " + sRightString+ " ON   (" + sCondition + ")" + " ) "+  aliasID;
        StateManager.setContext(context);

        //String sQuery = "from (  "  + sLeftString + " "+ sJoin + " " + sRightString+ " ON   (" + sCondition + ")" + " ) "+  aliasID;
        //String sQuery = "from (select * from " + sLeftString + " "+ sJoin + " " + sRightString+ " ON   (" + sCondition + ")" + " ) "+  aliasID;
        StateManager.setContext(context);
        result = new HashMap<String, String>();

        String leftInitializationScript = mapLeftStream.get(Constants.INITALIZATION_SCRIPT);
        String rightInitializationScript = mapRightStream.get(Constants.INITALIZATION_SCRIPT);

        String initializationScript = "";

        if(leftInitializationScript != null)
            initializationScript = leftInitializationScript + "\n";

        if(rightInitializationScript != null)
            initializationScript += rightInitializationScript + "\n";

        if(initializationScript.isEmpty() == false)
            result.put(Constants.INITALIZATION_SCRIPT, initializationScript);


        if(result.get(Constants.LENGTH_WINDOW_BATCH_FREQUENCY) != null)
            result.put(Constants.LENGTH_WINDOW_BATCH_FREQUENCY, result.get(Constants.LENGTH_WINDOW_BATCH_FREQUENCY) );


        if(result.get(Constants.LENGTH_WINDOW_FREQUENCY) != null)
            result.put(Constants.LENGTH_WINDOW_FREQUENCY, result.get(Constants.LENGTH_WINDOW_FREQUENCY) );

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
