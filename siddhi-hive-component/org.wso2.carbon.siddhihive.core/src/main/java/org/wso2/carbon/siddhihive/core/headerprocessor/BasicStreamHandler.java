package org.wso2.carbon.siddhihive.core.headerprocessor;


//import org.wso2.carbon.siddhihive.handler.ConditionHandler;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.query.input.BasicStream;
import org.wso2.siddhi.query.api.query.input.Stream;
import org.wso2.siddhi.query.api.query.input.handler.Filter;

import java.util.HashMap;
import java.util.Map;

public class BasicStreamHandler implements StreamHandler {

    private String fromClause;
    private String whereClause;
    private BasicStream basicStream;
    private Map<String, String> result;

    @Override
    public Map<String, String> process(Stream stream) {
        basicStream = (BasicStream) stream;
        fromClause = generateFromClause(basicStream.getStreamId());
        whereClause = generateWhereClause(basicStream.getFilter());
        result = new HashMap<String, String>();
        result.put(Constants.FROM_CLAUSE, fromClause);
        result.put(Constants.WHERE_CLAUSE, whereClause);
        return result;
    }

    public String generateFromClause(String streamId) {
        String clause = Constants.FROM + " " + streamId;
        return clause;
    }

    public String generateWhereClause(Filter filter) {
        if (filter != null) {
           // ConditionHandler conditionHandler = new ConditionHandler();
            //String filterStr = conditionHandler.processCondition(filter.getFilterCondition());
            return ""/*Constants.WHERE + " " + filterStr*/;
        } else {
            return "";
        }

    }
}
