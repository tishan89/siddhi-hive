package org.wso2.carbon.siddhihive.core.selectorprocessor;

/**
 * Created by Firzhan on 5/30/14.
 */

import org.wso2.carbon.siddhihive.core.configurations.Context;
import org.wso2.carbon.siddhihive.core.handler.AttributeHandler;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.internal.StateManager;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.enums.InputStreamProcessingLevel;
import org.wso2.carbon.siddhihive.core.utils.enums.SelectorProcessingLevel;
import org.wso2.siddhi.query.api.condition.AndCondition;
import org.wso2.siddhi.query.api.condition.Condition;
import org.wso2.siddhi.query.api.condition.OrCondition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.query.selection.Selector;
import org.wso2.siddhi.query.api.query.selection.attribute.ComplexAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.OutputAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.OutputAttributeExtension;
import org.wso2.siddhi.query.api.query.selection.attribute.SimpleAttribute;



import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class QuerySelectorProcessor {

    private ConditionHandler conditionHandler = null;
    private AttributeHandler attributeHandler = null;

    private ConcurrentMap<String, String> selectorQueryMap = null;
    private List<SimpleAttribute> simpleAttributeList = null;
    private ConcurrentMap<String, String> selectionStringMap = null; //rename and selection name


    public QuerySelectorProcessor() {

        conditionHandler = new ConditionHandler();
        attributeHandler = new AttributeHandler();

        selectorQueryMap = new ConcurrentHashMap<String, String>();
        simpleAttributeList = new ArrayList<SimpleAttribute>();

        selectionStringMap = new ConcurrentHashMap<String, String>();

    }

    public boolean handleSelector(Selector selector) {

        if (selector == null)
            return false;

        selectorQueryMap.clear();

        String selectionQuery = handleSelectionList(selector);
        String groupByQuery = handleGroupByList(selector);
        String handle = handleHavingCondition(selector);

        selectorQueryMap.put(Constants.SELECTION_QUERY, selectionQuery);
        selectorQueryMap.put(Constants.GROUP_BY_QUERY, groupByQuery);
        selectorQueryMap.put(Constants.HAVING_QUERY, handle);

        Context context = StateManager.getContext();

        context.setSelectorProcessingLevel(SelectorProcessingLevel.NONE);
        context.setInputStreamProcessingLevel(InputStreamProcessingLevel.NONE);

        StateManager.setContext(context);

        return true;
    }

    public ConcurrentMap<String, String> getSelectorQueryMap() {
        return selectorQueryMap;
    }

    private String handleSelectionList(Selector selector) {

        Context context = StateManager.getContext();
        context.setSelectorProcessingLevel(SelectorProcessingLevel.SELECTOR);
        StateManager.setContext(context);

        List<OutputAttribute> selectionList = selector.getSelectionList();

        int selectionListSize = selectionList.size();

        if (selectionListSize == 0)
            return " ";

        String selectionString = " ";

        for (int i = 0; i < selectionListSize; i++) {

            OutputAttribute outputAttribute = selectionList.get(i);

            postProcessAttributes(outputAttribute);

            if (outputAttribute instanceof SimpleAttribute) {
                selectionString += attributeHandler.handleSimpleAttribute((SimpleAttribute) outputAttribute);
            } else if (outputAttribute instanceof ComplexAttribute) {
                selectionString += attributeHandler.handleComplexAttribute((ComplexAttribute) outputAttribute);
            } else if (outputAttribute instanceof OutputAttributeExtension) {

            }

            if ((selectionListSize > 1) && ((i + 1) < selectionListSize))
                selectionString += " , ";
        }

        selectionString += ", timestamps";

        return selectionString;
    }

    private void postProcessAttributes(OutputAttribute outputAttribute){

        if(outputAttribute instanceof SimpleAttribute){
            simpleAttributeList.add((SimpleAttribute) outputAttribute);

            String selectionString = attributeHandler.handleSimpleAttribute((SimpleAttribute) outputAttribute);

            Context context = StateManager.getContext();
            context.addSelectionAttributeRename(outputAttribute.getRename(), selectionString);
            StateManager.setContext(context);
      }
    }

    private String handleGroupByList(Selector selector) {

        Context context = StateManager.getContext();
        context.setSelectorProcessingLevel(SelectorProcessingLevel.GROUPBY);
        StateManager.setContext(context);

        if( selector.getGroupByList().size() == 0)
            return "";
        String groupBy = " GROUP BY ";

        int groupByListSize = simpleAttributeList.size();

        for (int i = 0; i < groupByListSize; i++) {
            SimpleAttribute simpleAttribute = simpleAttributeList.get(i);
            Expression expression = simpleAttribute.getExpression();

            if (expression instanceof Variable) {
                groupBy += "  " + conditionHandler.handleVariable((Variable)expression);

                if ((groupByListSize > 1) && ((i + 1) < groupByListSize))
                    groupBy += " , ";
            }
        }
        groupBy += ", " + Constants.TIMESTAMPS_COLUMN;
        return groupBy;
    }

    private String handleHavingCondition(Selector selector) {

        String handleCondition = "  ";
        Condition condition = selector.getHavingCondition();

        if (condition == null)
            return " ";

        Context context = StateManager.getContext();
        context.setSelectorProcessingLevel(SelectorProcessingLevel.HAVING);
        StateManager.setContext(context);

        handleCondition = conditionHandler.processCondition(condition);

        if ((condition instanceof OrCondition) || (condition instanceof AndCondition))
            handleCondition = Constants.OPENING_BRACT + Constants.SPACE + handleCondition + Constants.SPACE + Constants.CLOSING_BRACT;

        handleCondition = Constants.HAVING + handleCondition;

        return handleCondition;
    }
}
