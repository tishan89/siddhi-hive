package org.wso2.carbon.siddhihive.core.handler;

import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.query.selection.attribute.ComplexAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.SimpleAttribute;

/**
 * Created by root on 6/3/14.
 */
public class AttributeHandler {

    private ConditionHandler conditionHandler = null;

    public AttributeHandler() {
        conditionHandler = new ConditionHandler();
    }

    public String handleSimpleAttribute(SimpleAttribute simpleAttribute) {

        String expressionValue = "";

        String rename = simpleAttribute.getRename();

        Expression expression = simpleAttribute.getExpression();

        if (expression instanceof Variable) {

            boolean multipleAttr = false;

            if (expressionValue.trim().equalsIgnoreCase("") == false)
                multipleAttr = true;

            Variable variable = (Variable) expression;
            expressionValue += conditionHandler.handleVariable(variable);

            if (variable.getAttributeName().equals(rename) == false) {
                expressionValue += " AS " + rename;
            }

            if (multipleAttr)
                expressionValue += ",";
        }


        return expressionValue;
    }


    public String handleComplexAttribute(ComplexAttribute complexAttribute) {

        String expressionValue = "";
        String rename = complexAttribute.getRename();
        String complexAttrName = complexAttribute.getAttributeName();

        Expression[] expressions = complexAttribute.getExpressions();

        int expressionLength = expressions.length;

        for (int i = 0; i < expressionLength; i++) {

            Expression expression = expressions[i];

            if (expression instanceof Variable) {

                boolean multipleAttr = false;

                if (expressionValue.trim().equalsIgnoreCase("") == false)
                    multipleAttr = true;


                Variable variable = (Variable) expression;
                expressionValue += " " + complexAttrName + "( " + conditionHandler.handleVariable(variable) + " )";

                if (variable.getAttributeName().equals(rename) == false) {
                    expressionValue += " AS " + rename;

                    if (multipleAttr)
                        expressionValue += ",";
                }
            }

        }

        return expressionValue;
    }

}
