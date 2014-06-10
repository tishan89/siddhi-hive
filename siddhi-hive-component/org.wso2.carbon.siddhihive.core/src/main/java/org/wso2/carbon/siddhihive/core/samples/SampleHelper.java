package org.wso2.carbon.siddhihive.core.samples;

import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SampleHelper {

    public ExecutionPlan getExecutionPlan(String query, List<String> streamDefList, List<String> fullQualifiedName) {
        if (streamDefList.size() != fullQualifiedName.size()) {
            SiddhiManager siddhiManager = new SiddhiManager();
            for (String definition : streamDefList) {
                siddhiManager.defineStream(definition);
            }
            List<StreamDefinition> siddhiDef = siddhiManager.getStreamDefinitions();
            Map<String, StreamDefinitionExt> siddhiHiveDef = new HashMap<String, StreamDefinitionExt>();
            for (int i = 0; i < siddhiDef.size(); i++) {
                siddhiHiveDef.put(siddhiDef.get(i).getStreamId(), new StreamDefinitionExt(fullQualifiedName.get(i), siddhiDef.get(i)));
            }
            ExecutionPlan executionPlan = new ExecutionPlan(query, siddhiHiveDef);
            return executionPlan;
        }
        return null;
    }
}
