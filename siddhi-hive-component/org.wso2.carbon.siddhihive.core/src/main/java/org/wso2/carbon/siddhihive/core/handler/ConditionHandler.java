package org.wso2.carbon.siddhihive.core.handler;

import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.InputStreamProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.ProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.SelectorProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.WindowProcessingLevel;
import org.wso2.siddhi.query.api.condition.*;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Multiply;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.*;

/**
 * Created by Firzhan on 6/2/14.
 */
public class ConditionHandler {

    private SiddhiHiveManager siddhiHiveManager;

    public ConditionHandler(SiddhiHiveManager siddhiHiveManager) {

        this.siddhiHiveManager = siddhiHiveManager;
    }

    public String processCondition(Condition condition) {

        String handleCondition = " ";

        if (condition == null)
            return " ";

        if (condition instanceof Compare) {
            handleCondition += handleCompareCondition((Compare) condition);
        } else if (condition instanceof AndCondition) {
            String leftCondition = processCondition(((AndCondition) condition).getLeftCondition());
            String rightCondition = processCondition(((AndCondition) condition).getRightCondition());
            handleCondition += Constants.SPACE + Constants.OPENING_BRACT + leftCondition + Constants.CLOSING_BRACT + Constants.SPACE + Constants.AND + Constants.SPACE + Constants.OPENING_BRACT + rightCondition + Constants.CLOSING_BRACT + Constants.SPACE;
        } else if (condition instanceof BooleanCondition) {
            //do
        } else if (condition instanceof InCondition) {
            //do
        } else if (condition instanceof NotCondition) {
            System.out.println("Accessed Not Condition");
        } else if (condition instanceof OrCondition) {
            String leftCondition = processCondition(((OrCondition) condition).getLeftCondition());
            String rightCondition = processCondition(((OrCondition) condition).getRightCondition());
            handleCondition += Constants.SPACE + Constants.OPENING_BRACT + leftCondition + Constants.CLOSING_BRACT + Constants.SPACE + Constants.OR + Constants.SPACE + Constants.OPENING_BRACT + rightCondition + Constants.CLOSING_BRACT + Constants.SPACE;
        }

        return handleCondition;

    }

    public String handleCompareCondition(Compare compare) {

        String leftExpressiveValue = handleCompareExpression(compare.getLeftExpression());
        String rightExpressiveValue = handleCompareExpression(compare.getRightExpression());


        String operatorString = getOperator(compare.getOperator());

        return " " + leftExpressiveValue + "  " + operatorString + "  " + rightExpressiveValue;

    }


    public String handleCompareExpression(Expression expression) {

        String expressionValue = " ";

        if (expression instanceof Variable) {
            expressionValue = handleVariable((Variable) expression);
        } else if (expression instanceof Multiply) {
            Multiply multiply = (Multiply) expression;
            expressionValue = handleCompareExpression(multiply.getLeftValue());
            expressionValue += " * ";
            expressionValue += handleCompareExpression(multiply.getRightValue());
            // expressionValue = ((Multiply)expression.getStreamId() != null ? (variable.getStreamId() + ".") : "") + variable.getAttributeName();
        } else if (expression instanceof Constant) {

            if (expression instanceof IntConstant) {
                IntConstant intConstant = (IntConstant) expression;
                expressionValue = intConstant.getValue().toString();
            } else if (expression instanceof DoubleConstant) {
                DoubleConstant doubleConstant = (DoubleConstant) expression;
                expressionValue = doubleConstant.getValue().toString();
            } else if (expression instanceof FloatConstant) {
                FloatConstant floatConstant = (FloatConstant) expression;
                expressionValue = floatConstant.getValue().toString();
            } else if (expression instanceof LongConstant) {
                LongConstant longConstant = (LongConstant) expression;
                expressionValue = longConstant.getValue().toString();
            }else if (expression instanceof StringConstant) {
                StringConstant stringConstant = (StringConstant) expression;
                expressionValue = "\""+ stringConstant.getValue().toString() + "\" ";
            }
        }

        return expressionValue;
    }

    public String handleVariable(Variable variable) {
        // return (variable.getStreaimId() != null ? (siddhiHiveManager.getStreamReferenceID(variable.getStreamId()) + ".") : variable.getStreamId()) + variable.getAttributeName();

        String variableName ="";
        String streamID = "";

        if( (siddhiHiveManager.getProcessingLevel() == ProcessingLevel.SELECTOR) && (siddhiHiveManager.getSelectorProcessingLevel() == SelectorProcessingLevel.HAVING) ){

                variableName = siddhiHiveManager.getSelectionAttributeRenameMap(variable.getAttributeName());

                if (variableName == null)
                    variableName = variable.getAttributeName();

        }else{

            if(siddhiHiveManager.getCachedValues("STREAM_ID") != null )
                streamID = siddhiHiveManager.getCachedValues("STREAM_ID");

            if(streamID.isEmpty() == false){
                variableName = streamID + ".";
            }else{

                if(variable.getStreamId() != null){
                    variableName = variable.getStreamId();
                    variableName +=".";
                }
            }

            variableName += variable.getAttributeName();
        }

//
//        if ( siddhiHiveManager.getProcessingLevel() == ProcessingLevel.SELECTOR_HAVING){
//            variableName = siddhiHiveManager.getSelectionAttributeRenameMap(variable.getAttributeName());
//        }
//        else{
//            if(siddhiHiveManager.getCachedValues("STREAM_ID") != null )
//                streamID = siddhiHiveManager.getCachedValues("STREAM_ID");
//
//            if(streamID.isEmpty() == false){
//                variableName = streamID;
//            }else{
//
//                if(variable.getStreamId() != null){
//                    variableName = variable.getStreamId();
//                    variableName +=".";
//                }
//                variableName += variable.getAttributeName();
//            }
//        }
//
//        if (variableName == null){
//            variableName = variable.getAttributeName();
//        }

//
//        if(variable.getStreamId() != null){
//
//
//        }
//        else{
//
//        }

//        if(variable.getStreamId() != null){
//
//          if(siddhiHiveManager.getWindowProcessingState() == WindowProcessingState.WINDOW_PROCESSED){
//
//              ProcessingLevel processingMode = siddhiHiveManager.getProcessingLevel();
//              if(processingMode == ProcessingLevel.SELECTOR){
//                  variableName = siddhiHiveManager.getCachedValues(variable.getStreamId());
//              }
//
//          }
//          else if(siddhiHiveManager.getWindowProcessingState() == WindowProcessingState.WINDOW_PROCESSING){
//
//              ProcessingLevel processingMode = siddhiHiveManager.getProcessingLevel();
//              if(processingMode == ProcessingLevel.SELECTOR_WHERE){
//                  variableName = siddhiHiveManager.getCachedValues(variable.getStreamId());
//              }
//          }
//          else{
//                if (siddhiHiveManager.getStreamReferenceID(variable.getStreamId()) != null ){
//                    variableName =  siddhiHiveManager.getStreamReferenceID(variable.getStreamId());
//                }
//                else {
//                    variableName = variable.getStreamId();
//                }
//            }
//
//            variableName += "."  ;
//            variableName += variable.getAttributeName();
//        }
//        else{
//            //if this is a having condition mode operator with null streamID
//            if (siddhiHiveManager.getProcessingLevel() == ProcessingLevel.SELECTOR_HAVING){
//
//            }
//
//            if(variableName == null)
//                variableName = variable.getAttributeName(); //This for safety. sort of Hack
//
//        }

        return variableName;
    }

    public String getOperator(Condition.Operator operator) {


        if (operator == Condition.Operator.EQUAL)
            return " = ";
        else if (operator == Condition.Operator.NOT_EQUAL)
            return " != ";
        else if (operator == Condition.Operator.GREATER_THAN)
            return " > ";
        else if (operator == Condition.Operator.GREATER_THAN_EQUAL)
            return " >= ";
        else if (operator == Condition.Operator.LESS_THAN)
            return " < ";
        else if (operator == Condition.Operator.LESS_THAN_EQUAL)
            return " <= ";
        else if (operator == Condition.Operator.CONTAINS)
            return " CONTAINS ";


//        else if(operator == Condition.Operator.INSTANCE_OF)
//            return " = ";


        return " ";
    }
}
