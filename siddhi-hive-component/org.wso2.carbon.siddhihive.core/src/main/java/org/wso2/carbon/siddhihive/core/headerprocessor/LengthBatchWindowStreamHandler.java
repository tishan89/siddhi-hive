package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel;
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

    private String firstSelectClause;
    private String secondSelectClause;
    private String wndSubQueryIdentifier = null;


    public LengthBatchWindowStreamHandler(SiddhiHiveManager siddhiHiveManagerParam) {
        super(siddhiHiveManagerParam);
    }

    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        this.windowStream = (WindowStream) stream;
        initializeWndVariables();


        initializationScript = generateInitializationScript();
        selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        limitClause = generateEndingPhrase();
        firstSelectClause = generateFirstSelectClause();
        secondSelectClause = generateSecondSelectClause();

        invokeGenerateWhereClause(windowStream.getFilter());
        fromClause = assembleWindowFromClause(); //  from
        result = new HashMap<String, String>();
        result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);
        result.put(Constants.INITALIZATION_SCRIPT, initializationScript);
        //getSiddhiHiveManager().setWindowProcessingState(WindowProcessingState.WINDOW_PROCESSED);

        return result;
    }

    private String generateWindowSelectClause(){

        StreamDefinition streamDefinition = windowStream.getStreamDefinition();

        String params = "";

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

            params += ", " + streamID + "." + Constants.TIMESTAMPS_COLUMN + "  ,  " + " setCounterAndTimestamp( " + streamID + "." + Constants.TIMESTAMPS_COLUMN + " )";
        }

        if(params.isEmpty())
            params = " * ";

        params = Constants.SELECT + "  " + params;

        return params;
    }

    private String generateEndingPhrase(){

        String orderBY = Constants.ORDER_BY + "  " + windowStream.getStreamId() + "." + Constants.TIMESTAMPS_COLUMN + "   " + "ASC" + "\n";
        String limit = "LIMIT ${hiveconf:LIMIT_COUNT}"+ "\n";
        return orderBY + limit ;
    }

    private String generateInitializationScript(){

        Expression expression = windowStream.getWindow().getParameters()[0];
        IntConstant intConstant = (IntConstant)expression;
        int length = intConstant.getValue();

        long time = 1402315118124l;

        String timeStamp = "set TIME_STAMP=" + String.valueOf(time) +";" + "\n";
        String maxLimit = "set MAX_LIMIT="+length+";"+ "\n";
        String opMode="set OP_MODE="+"\'NORML\'"+";" + "\n";
        String analyzer=" analyzer resolvePath(path=\"file://${CARBON_HOME}/repository/components/lib/udf_SiddhiHive.jar\");" + "\n";
        String hiveAux="set hive.aux.jars.path=${hiveconf:FILE_PATH};" + "\n";
        String tempFunction = "create temporary function setCounterAndTimestamp as 'org.wso2.siddhihive.udfunctions.UDFIncrementalCounter';"+"\n";
        String executionInitializerClass = "class org.wso2.siddhihive.analytics.ScriptExecutionInitializer;"+"\n";

        return timeStamp + maxLimit + opMode+analyzer + hiveAux + tempFunction + executionInitializerClass;
    }

    private void initializeWndVariables(){


        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( streamID.equalsIgnoreCase(streamReferenceID))
            wndSubQueryIdentifier =  getSiddhiHiveManager().generateSubQueryIdentifier();
        else
            wndSubQueryIdentifier = streamReferenceID;

        getSiddhiHiveManager().addStreamGeneratedQueryID(streamReferenceID, wndSubQueryIdentifier);
        //getSiddhiHiveManager().addCachedValues(this.windowStream.getStreamId(), wndSubQueryIdentifier);
        getSiddhiHiveManager().addCachedValues("STREAM_ID", wndSubQueryIdentifier);

        getSiddhiHiveManager().setWindowStreamProcessingLevel(WindowStreamProcessingLevel.LENGTH_BATCH_WINDOW_PROCESSING);

    }

    private String generateFirstSelectClause(){

        String clauseIdentifier = wndSubQueryIdentifier;

        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( !streamID.equalsIgnoreCase(streamReferenceID))
            clauseIdentifier = streamReferenceID;

        String fSelectClause = "SELECT * FROM ("
                                    +selectParamsClause + "       " + Constants.FROM + "  " + this.windowStream.getStreamId() + "  " + " WHERE " + Constants.TIMESTAMPS_COLUMN + " > " +
                                   "${hiveconf:TIME_STAMP}" + "\n" + limitClause + ")" + clauseIdentifier;

        return fSelectClause;
    }

    private String generateSecondSelectClause(){

        String aliasID = getSiddhiHiveManager().generateSubQueryIdentifier();

        getSiddhiHiveManager().setReferenceIDAlias(this.windowStream.getStreamReferenceId(),aliasID);
        getSiddhiHiveManager().addCachedValues("STREAM_ID", aliasID);

        return "SELECT * FROM ( \n" + this.firstSelectClause  + "\n ) " + aliasID + "\n";
    }

    public void invokeGenerateWhereClause(Filter filter) {
        getSiddhiHiveManager().setWindowProcessingLevel(WindowProcessingLevel.WND_WHERE_PROCESSING);
        whereClause = generateWhereClause(filter);
        getSiddhiHiveManager().setWindowProcessingLevel(WindowProcessingLevel.NONE);
    }

    private String assembleWindowFromClause(){

        if(whereClause.isEmpty())
            whereClause = " ";

        String aliasID = getSiddhiHiveManager().generateSubQueryIdentifier();

        getSiddhiHiveManager().setReferenceIDAlias(this.windowStream.getStreamReferenceId(),aliasID);
        getSiddhiHiveManager().addCachedValues("STREAM_ID", aliasID);


        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + secondSelectClause + "\n" + whereClause + Constants.CLOSING_BRACT + aliasID ;
    }
}
