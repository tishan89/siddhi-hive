package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.WindowProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.WindowStreamProcessingLevel;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by firzhan on 6/18/14.
 */
public class LengthBatchWindowStreamHandler extends WindowStreamHandler{

    private String windowIsolatorClause;
    private WindowIsolator windowIsolator;
    private WindowStream windowStream;
    private Map<String, String> result;

    private String initializationScript;
    private String fromClause;
    private String whereClause;
    private String selectParamsClause;
    private String limitClause;
    private String  schedulingFreq;

    private String firstSelectClause;
    private String secondSelectClause;
    private String wndSubQueryIdentifier = null;
    private String leftJoinfunctionCall = null;
    private String rightJoinfunctionCall = null;


    public LengthBatchWindowStreamHandler() {
    }

    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        result = new HashMap<String, String>();

        this.windowStream = (WindowStream) stream;
        initializeWndVariables();
        schedulingFreq = String.valueOf(Constants.DEFAULT_LENGTH_WINDOW_BATCH_FREQUENCY_TIME);

        initializationScript = generateInitializationScript();
        selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        limitClause = generateEndingPhrase();
        firstSelectClause = generateFirstSelectClause();
        secondSelectClause = generateSecondSelectClause();

        invokeGenerateWhereClause(windowStream.getFilter());
        fromClause = assembleWindowFromClause(); //  from

        result.put(Constants.LENGTH_BATCH_WIND_FROM_QUERY, fromClause);
        result.put(Constants.INITALIZATION_SCRIPT, initializationScript);

        if(leftJoinfunctionCall != null )
            result.put(Constants.FUNCTION_JOIN_LEFT_CALL_PARAM, leftJoinfunctionCall);

        if(rightJoinfunctionCall != null)
          result.put(Constants.FUNCTION_JOIN_RIGHT_CALL_PARAM, rightJoinfunctionCall);

        result.put(Constants.LENGTH_WINDOW_BATCH_FREQUENCY,schedulingFreq);
        //getSiddhiHiveManager().setWindowProcessingState(WindowProcessingState.WINDOW_PROCESSED);

        return result;
    }

    private String generateWindowSelectClause(){

        StreamDefinition streamDefinition = windowStream.getStreamDefinition();

        String params = "";

        Context context = StateManager.getContext();

        if(streamDefinition != null){

            ArrayList<Attribute> attributeArrayList = (ArrayList<Attribute>) streamDefinition.getAttributeList();

            String streamID = windowStream.getStreamId();
            for(int i=0; i < attributeArrayList.size(); ++i){

                Attribute attribute = attributeArrayList.get(i);

                if( params.isEmpty())
                    params += "  " + streamID + "." + attribute.getName() + " ";
                else
                    params += " , " + streamID + "." + attribute.getName() + " ";
            }

            params += ", " + streamID + "." + Constants.TIMESTAMPS_COLUMN + "  " ;
        }

        if(params.isEmpty())
            params = " * ";

        params = Constants.SELECT + "  " + params;

        return params;
    }

    private String generateEndingPhrase(){

        Context context = StateManager.getContext();

        Expression expression = windowStream.getWindow().getParameters()[0];
        IntConstant intConstant = (IntConstant)expression;
        int length = intConstant.getValue();


        String orderBY = Constants.ORDER_BY + "  " + windowStream.getStreamId() + "." + Constants.TIMESTAMPS_COLUMN + "   " + "ASC" + "\n";
        String limit = "LIMIT " +  length + "\n";
        return orderBY + limit ;
    }

    private String generateInitializationScript(){

        Context context = StateManager.getContext();

        context.generateTimeStampCounter(true);
        context.generateLimitCounter(true);
        StateManager.setContext(context);

        if(context.getIsScheduled() == false){
            Expression expression = windowStream.getWindow().getParameters()[0];
            IntConstant intConstant = (IntConstant)expression;
            int length = intConstant.getValue();

            long time = 1402315118124l;


           // String timeStamp = "set TIME_STAMP_" + context.generateTimeStampCounter(false)+"=" + String.valueOf(time) +";" + "\n";//INITIAL_TIMESTAMP
            //String timeStamp = "set INITIAL_TIMESTAMP_" + context.generateTimeStampCounter(false)+"=" + String.valueOf(time) +";" + "\n";
            //String maxLimit = "set MAX_LIMIT_" + context.generateLimitCounter(false) + "=" + length +";"+ "\n";
            String maxLimit = "set MAX_LIMIT_COUNT_" + context.generateLimitCounter(false) + "=" + length +";"+ "\n";
            String totalTimeStampCount = "set TOTAL_TIME_STAMP_COUNT="+ context.generateTimeStampCounter(false)+";" + "\n";
           // String totalLimitStampCount = "set TOTAL_LENGTH_COUNT="+ context.generateLimitCounter(false)+";" + "\n";
            //String limitCount = "set LIMIT_COUNT__" + context.generateLimitCounter(false) + "=" + length +";"+ "\n";

            return maxLimit + totalTimeStampCount ;
        }

        return  " ";
    }

//    private void oneTimeExecutionScriptOnSchedule(){
//
//        Context context = StateManager.getContext();
//
//        context.generateTimeStampCounter(true);
//        context.generateLimitCounter(true);
//
//        if(context.getIsScheduled() == false){
//            Expression expression = windowStream.getWindow().getParameters()[0];
//            IntConstant intConstant = (IntConstant)expression;
//            int length = intConstant.getValue();
//
//            long time = 1402315118124l;
//
//
//            String timeStamp = "set TIME_STAMP_" + context.generateTimeStampCounter(false)+"=" + String.valueOf(time) +";" + "\n";
//            String maxLimit = "set MAX_LIMIT_" + context.generateLimitCounter(false) + "=" + length +";"+ "\n";
//            String limitCount = "set LIMIT_COUNT__" + context.generateLimitCounter(false) + "=" + length +";"+ "\n";
//
////            String analyzer=" analyzer resolvePath(path=\"file://${CARBON_HOME}/repository/components/lib/udf_SiddhiHive.jar\");" + "\n";
////            String hiveAux="set hive.aux.jars.path=${hiveconf:FILE_PATH};" + "\n";
////            String tempFunction = "create temporary function setCounterAndTimestamp as 'org.wso2.siddhihive.udfunctions.UDFIncrementalCounter';"+"\n";
////            String executionInitializerClass = "class org.wso2.siddhihive.analytics.ScriptExecutionInitializer;"+"\n";
//        }
//
//
//        StateManager.setContext(context);
//    }

    private void initializeWndVariables(){

        Context context = StateManager.getContext();

        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( streamID.equalsIgnoreCase(streamReferenceID))
            wndSubQueryIdentifier =  context.generateSubQueryIdentifier();
        else
            wndSubQueryIdentifier = streamReferenceID;

        //getSiddhiHiveManager().addStreamGeneratedQueryID(streamReferenceID, wndSubQueryIdentifier);
        //getSiddhiHiveManager().addCachedValues(this.windowStream.getStreamId(), wndSubQueryIdentifier);
        //getSiddhiHiveManager().addCachedValues("STREAM_ID", wndSubQueryIdentifier);
        context.setReferenceIDAlias(streamReferenceID, wndSubQueryIdentifier);
        context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.LENGTH_BATCH_WINDOW_PROCESSING);

        StateManager.setContext(context);

    }

    private String generateFirstSelectClause(){

        Context context = StateManager.getContext();

        String clauseIdentifier = wndSubQueryIdentifier;

        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( !streamID.equalsIgnoreCase(streamReferenceID))
            clauseIdentifier = streamReferenceID;

        String fSelectClause = "SELECT * "+ " FROM (" +
                                    selectParamsClause + "       " + Constants.FROM + "  " + this.windowStream.getStreamId() + "  " + " WHERE " + Constants.TIMESTAMPS_COLUMN + " > " +
                                     "${hiveconf:TIMESTAMP_TO_BE_PROCESSESED_"+context.generateTimeStampCounter(false) + "}" + "\n" + limitClause + ")" + clauseIdentifier;

        return fSelectClause;
    }

    private String generateSecondSelectClause(){

        Context context = StateManager.getContext();

        String aliasID = context.generateSubQueryIdentifier();

        context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);
        //context.addCachedValues("STREAM_ID", aliasID);

        StateManager.setContext(context);


        return "SELECT * FROM ( \n" + this.firstSelectClause  + "\n ) " + aliasID + "\n";
    }

    public void invokeGenerateWhereClause(Filter filter) {

        Context context = null; 
        context = StateManager.getContext();
        context.setWindowProcessingLevel(WindowProcessingLevel.WND_WHERE_PROCESSING);
        StateManager.setContext(context);
        whereClause = generateWhereClause(filter);
        context = StateManager.getContext();
        context.setWindowProcessingLevel(WindowProcessingLevel.NONE);
        StateManager.setContext(context);
    }

    private String assembleWindowFromClause(){

        if(whereClause.isEmpty())
            whereClause = " ";

        Context context = StateManager.getContext();

        String aliasID = context.generateSubQueryIdentifier();
        String prveiousAliasID = context.generatePreviousSubQueryIdentifier();
        context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);

        StateManager.setContext(context);
//FUNCTION_JOIN_RIGHT_CALL_PARAM
        this.leftJoinfunctionCall = " setCounterAndTimestamp( " + context.generateTimeStampCounter(false) +", "+ aliasID + "." + Constants.TIMESTAMPS_COLUMN + " )";
        this.rightJoinfunctionCall = " setCounterAndTimestamp( " + context.generateTimeStampCounter(false) +", "+ prveiousAliasID + "." + Constants.TIMESTAMPS_COLUMN + " )";

        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + secondSelectClause + "\n" + whereClause + Constants.CLOSING_BRACT + aliasID ;
    }
}
