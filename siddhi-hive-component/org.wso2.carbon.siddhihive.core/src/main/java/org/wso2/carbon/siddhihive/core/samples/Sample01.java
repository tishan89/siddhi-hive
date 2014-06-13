package org.wso2.carbon.siddhihive.core.samples;

import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

import java.util.ArrayList;
import java.util.List;


public class Sample01 {


    private static String streamdef1 = "define stream KPIStream ( brand string, quantity int, total int, user string )";
    //private static String streamdef2 = "define stream KPIResultStream ( symbol string, avgPrice double )";
    private static String query1 = " from KPIStream[quantity >= 2]#window.time(1 min) " +
            " select brand, sum(total) as sumTotal " +
            " group by brand having sumTotal>200000" +
            " insert into KPIResultStream;";
    private static String fullName1 = "org.wso2.bam.phone.retail.store.kpi.test";
    //private static String fullName2 = "org.wso2.bam.phone.retail.store.kpi.test.result";
    private static List<String> defList = new ArrayList<String>();
    private static List<String> nameList = new ArrayList<String>();

    public static void main(String[] args) {
        defList.add(streamdef1);
        //defList.add(streamdef2);
        nameList.add(fullName1);
        //nameList.add(fullName2);
        SampleHelper sampleHelper = new SampleHelper();
        ExecutionPlan executionPlan = sampleHelper.getExecutionPlan(query1, defList, nameList);
        SiddhiHiveService siddhiHiveService = new SiddhiHiveService();
        String result = siddhiHiveService.addExecutionPlan(executionPlan);
        System.out.println(result);
    }
}
