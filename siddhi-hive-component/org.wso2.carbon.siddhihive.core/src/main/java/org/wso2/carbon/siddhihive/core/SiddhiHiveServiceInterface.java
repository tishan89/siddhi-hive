package org.wso2.carbon.siddhihive.core;

import org.wso2.carbon.event.processor.core.ExecutionPlanConfiguration;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

/**
 * This is the Siddhi-HiveServiceInterface to access Siddhi-HiveService functinalites from service consumer.
 */
public interface SiddhiHiveServiceInterface {

    /**
     * This method accepts an event processor execution plan configuration and generate the corresponding hive query
     * @param configuration : Execution plan configuration
     * @return : operation status
     */
public Boolean addExecutionPlan(ExecutionPlanConfiguration configuration, int tenantId);
public Boolean addStreamDefinition(StreamDefinition streamDefinition, String fullQualifiedname);
}
