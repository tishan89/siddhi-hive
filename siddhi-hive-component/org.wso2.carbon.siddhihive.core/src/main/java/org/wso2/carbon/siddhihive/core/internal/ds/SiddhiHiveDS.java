package org.wso2.carbon.siddhihive.core.internal.ds;


import org.apache.log4j.Logger;
import org.osgi.service.component.ComponentContext;
import org.wso2.carbon.event.stream.manager.core.EventStreamService;
import org.wso2.carbon.siddhihive.core.SiddhiHiveServiceInterface;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

/**
 * @scr.component name="siddhiHive.component" immediate="true"
 * @scr.reference name="eventStreamManager.service"
 * interface="org.wso2.carbon.event.stream.manager.core.EventStreamService" cardinality="1..1"
 * policy="dynamic" bind="setEventStreamManagerService" unbind="unsetEventStreamManagerService"
 */
public class SiddhiHiveDS {
    private static final Logger log = Logger.getLogger(SiddhiHiveDS.class);

    /**
     * Exposing SiddhiHive OSGi service.
     *
     * @param context
     */

    protected void activate(ComponentContext context) {

        try {
            SiddhiHiveService siddhiHiveService = new SiddhiHiveService();
            SiddhiHiveValueHolder.getInstance().registerSiddhiHiveService(siddhiHiveService);
            context.getBundleContext().registerService(SiddhiHiveServiceInterface.class.getName(),
                    siddhiHiveService, null);
            log.info("Successfully deployed the Siddhi-Hive conversion Service");
        } catch (Throwable e) {
            log.error("Can not create the Siddhi-Hive conversion Service ", e);
        }
    }

    protected void deactivate(ComponentContext context) {
        //context.getBundleContext().ungetService();
    }

    protected void setEventStreamManagerService(EventStreamService eventStreamService) {
        SiddhiHiveValueHolder.getInstance().setEventStreamService(eventStreamService);
    }

    protected void unsetEventStreamManagerService(EventStreamService eventStreamService) {
        SiddhiHiveValueHolder.getInstance().unsetEventStreamService();
    }
}
