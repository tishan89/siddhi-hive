package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.ProcessingMode;
import org.wso2.carbon.siddhihive.core.utils.WindowProcessingState;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;

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

    private int subqueryCounter = 0;


    public LengthBatchWindowStreamHandler(SiddhiHiveManager siddhiHiveManagerParam) {
        super(siddhiHiveManagerParam);
    }

    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        this.windowStream = (WindowStream) stream;
        addStreamReference(this.windowStream.getStreamReferenceId(), this.windowStream.getStreamId());


        initializationScript = generateInitializationScript();
        selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        limitClause = generateLimitLength();


        //getSubQueryIdentifier(true);
        getSiddhiHiveManager().setProcessingMode(ProcessingMode.SELECTOR_WHERE);
        whereClause = generateWhereClause(windowStream.getFilter()); //where   A.symbol   =   "IBM"
        getSiddhiHiveManager().setProcessingMode(ProcessingMode.INPUT_STREAM);
        //fromClause = assembleWindowFromClause(); //  from
        result = new HashMap<String, String>();
        result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);

        getSiddhiHiveManager().setWindowProcessingState(WindowProcessingState.WINDOW_PROCESSED);

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

    private String generateLimitLength(){

        Expression expression = windowStream.getWindow().getParameters()[0];
        IntConstant intConstant = (IntConstant)expression;
        int length = intConstant.getValue();

        return Constants.ORDER_BY + "  " + String.valueOf(length);
    }

    private String generateInitializationScript(){

        Expression expression = windowStream.getWindow().getParameters()[0];
        IntConstant intConstant = (IntConstant)expression;
        int length = intConstant.getValue();

        String maxLimit = "set MAX_LIMIT="+length+";"+ "\n";
        String opMode="set OP_MODE="+"\'NORML\'"+";" + "\n";
        String analyzer=" analyzer resolvePath(path=\"file://${CARBON_HOME}/repository/components/lib/udf_SiddhiHive.jar\");" + "\n";
        String hiveAux="set hive.aux.jars.path=${hiveconf:FILE_PATH};" + "\n";
        String tempFunction = "create temporary function setCounterAndTimestamp as 'org.wso2.siddhihive.udfunctions.UDFIncrementalCounter';"+"\n";
        String executionInitializerClass = "class org.wso2.siddhihive.analytics.ScriptExecutionInitializer;"+"\n";

        return maxLimit + opMode+analyzer + hiveAux + tempFunction + executionInitializerClass;
    }
}
