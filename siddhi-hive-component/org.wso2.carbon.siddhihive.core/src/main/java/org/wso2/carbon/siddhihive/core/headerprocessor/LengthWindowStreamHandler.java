package org.wso2.carbon.siddhihive.core.headerprocessor;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.utils.Constants;
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

    public LengthWindowStreamHandler(){

        this.windowIsolator = new WindowIsolator();
    }
    public Map<String, String> process(Stream stream, Map<String, StreamDefinitionExt> streamDefinitions){

        this.windowStream = (WindowStream) stream;


        result = new HashMap<String, String>();


        whereClause = generateWhereClause(windowStream.getFilter());
        selectParamsClause = generateWindowSelectClause();
        limitClause = generateLimitLength();

        fromClause = assembleFromClause();

        result.put(Constants.LENGTH_WIND_FROM_QUERY, fromClause);
        return result;
    }

    private String assembleFromClause(){

        if(whereClause.isEmpty())
            whereClause = " ";

        return Constants.FROM + "  " + Constants.OPENING_BRACT + "   " + selectParamsClause + " " + whereClause + "  " + limitClause + "   " + Constants.CLOSING_BRACT + "     A";
    }

    private String generateWindowSelectClause(){

        StreamDefinition streamDefinition = windowStream.getStreamDefinition();

        String params = "";

        if(streamDefinition != null){

            ArrayList<Attribute> attributeArrayList = (ArrayList<Attribute>) streamDefinition.getAttributeList();

            String streamId = streamDefinition.getStreamId();

            for(int i=0; i < attributeArrayList.size(); ++i){

                Attribute attribute = attributeArrayList.get(i);

                if( params.isEmpty())
                    params += "  " + streamId + "." + attribute.getName() + " ";
                else
                    params += " , " + streamId + "." + attribute.getName() + " ";
            }

            params += ", " + Constants.TIMESTAMPS_COLUMN + " ";
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
