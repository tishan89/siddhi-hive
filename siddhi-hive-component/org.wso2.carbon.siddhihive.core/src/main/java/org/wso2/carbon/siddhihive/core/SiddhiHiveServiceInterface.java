package org.wso2.carbon.siddhihive.core;

import org.wso2.carbon.event.processor.core.ExecutionPlanConfiguration;
import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.Map;

/**
 * This is the Siddhi-HiveServiceInterface to access Siddhi-HiveService functinalites from service consumer.
 */
public interface SiddhiHiveServiceInterface {

    /**
     * This method accepts an event processor execution plan configuration and generate the corresponding hive query
     * @param executionPlan : Execution plan configuration
     * @return : Generated Hive query
     */
    public String addExecutionPlan(ExecutionPlan executionPlan);
//public Boolean addStreamDefinition(StreamDefinition streamDefinition, String fullQualifiedName);
}
