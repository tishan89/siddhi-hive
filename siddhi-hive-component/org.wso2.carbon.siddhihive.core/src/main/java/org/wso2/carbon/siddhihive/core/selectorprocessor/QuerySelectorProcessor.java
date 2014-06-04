package org.wso2.carbon.siddhihive.core.selectorprocessor;

/**
 * Created by Firzhan on 5/30/14.
 */

import org.wso2.carbon.siddhihive.core.handler.AttributeHandler;
import org.wso2.carbon.siddhihive.core.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.utils.Constants;
import org.wso2.siddhi.query.api.condition.AndCondition;
import org.wso2.siddhi.query.api.condition.Condition;
import org.wso2.siddhi.query.api.condition.OrCondition;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.query.selection.Selector;
import org.wso2.siddhi.query.api.query.selection.attribute.ComplexAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.OutputAttribute;
import org.wso2.siddhi.query.api.query.selection.attribute.OutputAttributeExtension;
import org.wso2.siddhi.query.api.query.selection.attribute.SimpleAttribute;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class QuerySelectorProcessor {

    private ConditionHandler conditionHandler = null;
    private AttributeHandler attributeHandler = null;
    private ConcurrentMap<String, String> selectorQueryMap = null;

    public QuerySelectorProcessor() {

        conditionHandler = new ConditionHandler();
        attributeHandler = new AttributeHandler();

        selectorQueryMap = new ConcurrentHashMap<String, String>();
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

        return true;
    }

    public ConcurrentMap<String, String> getSelectorQueryMap() {
        return selectorQueryMap;
    }

    private String handleSelectionList(Selector selector) {

        List<OutputAttribute> selectionList = selector.getSelectionList();

        int selectionListSize = selectionList.size();

        if (selectionListSize == 0)
            return " ";

        String selectionString = " ";

        for (int i = 0; i < selectionListSize; i++) {

            OutputAttribute outputAttribute = selectionList.get(i);

            if (outputAttribute instanceof SimpleAttribute) {
                selectionString += attributeHandler.handleSimpleAttribute((SimpleAttribute) outputAttribute);
            } else if (outputAttribute instanceof ComplexAttribute) {
                selectionString += attributeHandler.handleComplexAttribute((ComplexAttribute) outputAttribute);
            } else if (outputAttribute instanceof OutputAttributeExtension) {

            }

            if ((selectionListSize > 1) && ((i + 1) < selectionListSize))
                selectionString += " , ";
        }

        return selectionString;
    }

    private String handleGroupByList(Selector selector) {

        String groupBy = " GROUP BY ";
        List<Variable> groupByList = selector.getGroupByList();

        int groupByListSize = groupByList.size();

        if (groupByListSize == 0)
            return " ";

        for (int i = 0; i < groupByListSize; i++) {

            Variable variable = groupByList.get(i);

            groupBy += "  " + conditionHandler.handleVariable(variable);

            if ((groupByListSize > 1) && ((i + 1) < groupByListSize))
                groupBy += " , ";


        }

        return groupBy;
    }

    private String handleHavingCondition(Selector selector) {

        String handleCondition = "  ";
        Condition condition = selector.getHavingCondition();

        if (condition == null)
            return " ";

        handleCondition = conditionHandler.processCondition(condition);

        if ((condition instanceof OrCondition) || (condition instanceof AndCondition))
            handleCondition = Constants.OPENING_BRACT + Constants.SPACE + handleCondition + Constants.SPACE + Constants.CLOSING_BRACT;

        handleCondition = Constants.HAVING + handleCondition;

        return handleCondition;
    }
}
