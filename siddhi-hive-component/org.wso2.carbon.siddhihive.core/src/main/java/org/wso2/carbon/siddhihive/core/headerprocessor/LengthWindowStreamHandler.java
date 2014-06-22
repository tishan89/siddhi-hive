package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
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
 * Created by firzhan on 6/10/14.
 */
public class LengthWindowStreamHandler extends WindowStreamHandler {

    private String windowIsolatorClause;
    private WindowIsolator windowIsolator;
    private WindowStream windowStream;
    private Map<String, String> result;

    private String fromClause;
    private String whereClause;
    private String selectParamsClause;
    private String limitClause;
    private String  schedulingFreq;

    private String wndSubQueryIdentifier = null;

    private String firstSelectClause = null;
    private String secondSelectClause = null;

   // private int subqueryCounter = 0;

    public LengthWindowStreamHandler(){

        this.windowIsolator = new WindowIsolator();
    }

    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        this.windowStream = (WindowStream) stream;

        schedulingFreq = String.valueOf(Constants.LENGTH_WINDOW_FREQUENCY_TIME);
        initializeWndVariables();
        selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        limitClause = generateLimitLength();

        invokeGenerateWhereClause(windowStream.getFilter()); //where   A.symbol   =   "IBM"
        firstSelectClause = generateFirstSelectClause();
       // secondSelectClause = generateSecondSelectClause();

        fromClause = assembleWindowFromClause(); //  from
        result = new HashMap<String, String>();
        result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);
        result.put(Constants.LENGTH_WINDOW_FREQUENCY,schedulingFreq);

        finalizeWndVariable();
        return result;
    }

    private String assembleWindowFromClause(){

        if(whereClause.isEmpty())
            whereClause = " ";

        Context context = StateManager.getContext();

        String aliasID = context.generateSubQueryIdentifier();

        context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);

        StateManager.setContext(context);
        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + firstSelectClause + "\n" + whereClause + Constants.CLOSING_BRACT + aliasID ;
//        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + selectParamsClause + " " + Constants.FROM  + "  " + windowStream.getStreamId()  + limitClause + "   " + Constants.CLOSING_BRACT + wndSubQueryIdentifier +
//               "\n" + " " + whereClause + "  ";
    }

    private void initializeWndVariables(){


        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        Context context = StateManager.getContext();

        if( streamID.equalsIgnoreCase(streamReferenceID))
            wndSubQueryIdentifier =  context.generateSubQueryIdentifier();
        else
            wndSubQueryIdentifier = streamReferenceID;

        //context.addStreamGeneratedQueryID(streamReferenceID,wndSubQueryIdentifier);
        //getSiddhiHiveManager().addCachedValues(this.windowStream.getStreamId(), wndSubQueryIdentifier);
        //context.addCachedValues("STREAM_ID", wndSubQueryIdentifier);
        context.setReferenceIDAlias(streamReferenceID, wndSubQueryIdentifier);
        context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.LENGTH_WINDOW_PROCESSING);

        StateManager.setContext(context);
//        LengthWndStreamInfo
// .Holder lengthWndStreamInfoHolder = new StreamRelationshipHolder();
//        lengthWndStreamInfoHolder.setStreamReferenceID(streamReferenceID);
//        lengthWndStreamInfoHolder.initializeWndVariables(subQueryIdentifier);
//        lengthWndStreamInfoHolder.setStreamID(streamID);
//        getSiddhiHiveManager().setLengthWndStreamInfoHolder(streamReferenceID, lengthWndStreamInfoHolder);


       // return subQueryIdentifier;
    }

    private void finalizeWndVariable(){

        Context context = StateManager.getContext();
        context.setWindowStreamProcessingLevel(WindowStreamProcessingLevel.NONE);
        context.setWindowProcessingLevel(WindowProcessingLevel.NONE);
        StateManager.setContext(context);
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

            params += ", " + streamID + "." + Constants.TIMESTAMPS_COLUMN + " ";
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

        String orderBY = Constants.ORDER_BY + "  " + windowStream.getStreamId() + "." + Constants.TIMESTAMPS_COLUMN + "   " + "ASC" + "\n";
        String limit = "LIMIT " + String.valueOf(length) + "\n";
        return orderBY + limit ;

    }

    private String generateFirstSelectClause(){

        String clauseIdentifier = wndSubQueryIdentifier;

        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( !streamID.equalsIgnoreCase(streamReferenceID))
            clauseIdentifier = streamReferenceID;

        String fSelectClause = "SELECT * FROM ("
                +selectParamsClause + "       " + Constants.FROM + "  " + this.windowStream.getStreamId() + "  " + limitClause + ")" + clauseIdentifier;

        return fSelectClause;
    }

    public void invokeGenerateWhereClause(Filter filter) {

       Context context = StateManager.getContext();
       context.setWindowProcessingLevel(WindowProcessingLevel.WND_WHERE_PROCESSING);
       StateManager.setContext(context);
       whereClause = generateWhereClause(filter);
       context = StateManager.getContext();
       context.setWindowProcessingLevel(WindowProcessingLevel.NONE);
       StateManager.setContext(context);

    }

    private String generateSecondSelectClause(){

        Context context = StateManager.getContext(); 
        String aliasID = context.generateSubQueryIdentifier();

        context.setReferenceIDAlias(this.windowStream.getStreamReferenceId(), aliasID);
       // context.addCachedValues("STREAM_ID", aliasID);

        StateManager.setContext(context);

        return "SELECT * FROM ( \n" + this.firstSelectClause  + "\n ) " + aliasID + "\n";
    }


}
