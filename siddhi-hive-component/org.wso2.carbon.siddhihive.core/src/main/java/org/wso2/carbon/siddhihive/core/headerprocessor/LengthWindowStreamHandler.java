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
import org.wso2.siddhi.query.api.query.input.handler.Window;

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

    private int subqueryCounter = 0;

    public LengthWindowStreamHandler(SiddhiHiveManager siddhiHiveManagerParam){

        super(siddhiHiveManagerParam);
        this.windowIsolator = new WindowIsolator();
    }

    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        this.windowStream = (WindowStream) stream;
        addStreamReference(this.windowStream.getStreamReferenceId(), this.windowStream.getStreamId());



        //update the SiddhiManager with abstraction details with referenceID <---> generated query t --> subq1

        String type = windowStream.getWindow().getName();

        //if( type.equals(Constants.LENGTH_WINDOW)){
            selectParamsClause = generateWindowSelectClause(); //SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps
        //}

       // if(! type.equals(Constants.LENGTH_WINDOW))    {



        //temporarily enable them
//            String streamID = windowStream.getStreamId();
//            this.getSiddhiHiveManager().addCachedValues(streamID, getSubQueryIdentifier(false));
            whereClause = generateWhereClause(windowStream.getFilter()); //where   A.symbol   =   "IBM"

            limitClause = generateLimitLength();

            fromClause = assembleWindowFromClause(); //  from
        //(
          //      SELECT     StockExchangeStream.symbol  , StockExchangeStream.price , StockExchangeStream.timestamps  ORDER BY timestamps DESC LIMIT   1
        //)      A



            result = new HashMap<String, String>();
            result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);

            getSiddhiHiveManager().setWindowProcessingState(WindowProcessingState.WINDOW_PROCESSED);

       // }
        return result;
    }

    private String assembleWindowFromClause(){

        if(whereClause.isEmpty())
            whereClause = " ";

        String subQueryIdentifier = "";

        String streamReferenceID = this.windowStream.getStreamReferenceId();
        String streamID =  this.windowStream.getStreamId();

        if( streamID.equalsIgnoreCase(streamReferenceID))
            subQueryIdentifier = getSubQueryIdentifier(true);
        else
            subQueryIdentifier = streamReferenceID;

        getSiddhiHiveManager().addStreamGeneratedQueryID(streamReferenceID,subQueryIdentifier);
        getSiddhiHiveManager().addCachedValues(this.windowStream.getStreamId(), subQueryIdentifier);
        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + selectParamsClause + " " + Constants.FROM  + "  " + windowStream.getStreamId() + " " + whereClause + "  " + limitClause + "   " + Constants.CLOSING_BRACT + subQueryIdentifier;
    }

    private String getSubQueryIdentifier(boolean generateID){

        String subqueryIdentifier = "subq";

        if(generateID)
            subqueryIdentifier += String.valueOf(++subqueryCounter);
        else
            subqueryIdentifier += String.valueOf(subqueryCounter);

        return subqueryIdentifier;
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

    }
