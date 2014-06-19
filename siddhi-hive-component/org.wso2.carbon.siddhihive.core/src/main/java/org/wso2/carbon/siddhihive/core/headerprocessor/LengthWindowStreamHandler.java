package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.LengthWndStreamInfoHolder;
import org.wso2.carbon.siddhihive.core.utils.ProcessingMode;
import org.wso2.carbon.siddhihive.core.utils.WindowProcessingState;
import org.wso2.siddhi.query.api.condition.Condition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.WindowStream;
import org.wso2.siddhi.query.api.query.input.handler.Window;
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

    private String wndSubQueryIdentifier = null;

   // private int subqueryCounter = 0;

    public LengthWindowStreamHandler(SiddhiHiveManager siddhiHiveManagerParam){

        super(siddhiHiveManagerParam);
        this.windowIsolator = new WindowIsolator();
    }

    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        this.windowStream = (WindowStream) stream;
        addStreamReference(this.windowStream.getStreamReferenceId(), this.windowStream.getStreamId());


        String type = windowStream.getWindow().getName();


        selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        limitClause = generateLimitLength();


        setSubQueryIdentifier();
        getSiddhiHiveManager().setProcessingMode(ProcessingMode.SELECTOR_WHERE);
        getSiddhiHiveManager().addCachedValues("STREAM_ID", wndSubQueryIdentifier);
        whereClause = generateWhereClause(windowStream.getFilter()); //where   A.symbol   =   "IBM"
        getSiddhiHiveManager().removedCachedValues("STREAM_ID");
        getSiddhiHiveManager().setProcessingMode(ProcessingMode.INPUT_STREAM);
        fromClause = assembleWindowFromClause(); //  from
        result = new HashMap<String, String>();
        result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);

        getSiddhiHiveManager().setWindowProcessingState(WindowProcessingState.WINDOW_PROCESSED);

        return result;
    }

    private String assembleWindowFromClause(){

        if(whereClause.isEmpty())
            whereClause = " ";

        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + selectParamsClause + " " + Constants.FROM  + "  " + windowStream.getStreamId()  + limitClause + "   " + Constants.CLOSING_BRACT + wndSubQueryIdentifier +
               "\n" + " " + whereClause + "  ";
    }

//    private String generateSubQueryIdentifier(boolean generateID){
//
//        String subQueryIdentifier = "subq";
//
//        if(generateID)
//            subQueryIdentifier += String.valueOf(++subqueryCounter);
//        else
//            subQueryIdentifier += String.valueOf(subqueryCounter);
//
//        return subQueryIdentifier;
//    }

    private void setSubQueryIdentifier(){


        if(wndSubQueryIdentifier == null){
            String streamReferenceID = this.windowStream.getStreamReferenceId();
            String streamID =  this.windowStream.getStreamId();

            if( streamID.equalsIgnoreCase(streamReferenceID))
                wndSubQueryIdentifier =  getSiddhiHiveManager().generateSubQueryIdentifier();
            else
                wndSubQueryIdentifier = streamReferenceID;

            getSiddhiHiveManager().addStreamGeneratedQueryID(streamReferenceID,wndSubQueryIdentifier);
            getSiddhiHiveManager().addCachedValues(this.windowStream.getStreamId(), wndSubQueryIdentifier);
        }

//        LengthWndStreamInfoHolder lengthWndStreamInfoHolder = new LengthWndStreamInfoHolder();
//        lengthWndStreamInfoHolder.setStreamReferenceID(streamReferenceID);
//        lengthWndStreamInfoHolder.setSubQueryIdentifier(subQueryIdentifier);
//        lengthWndStreamInfoHolder.setStreamID(streamID);
//        getSiddhiHiveManager().setLengthWndStreamInfoHolder(streamReferenceID, lengthWndStreamInfoHolder);


       // return subQueryIdentifier;
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

        return Constants.ORDER_BY + "  " + String.valueOf(length);
    }

//    private void modifyObjects(Object object){
//
//
//        if(object instanceof  Filter){
//
//            Filter filter = (Filter)object;
//
//            Condition condition = filter.getFilterCondition();
//
//            if( condition != null){
//
//                condition
//            }
//        }
//
//    }

    }
