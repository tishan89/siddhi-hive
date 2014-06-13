package org.wso2.carbon.siddhihive.core.samples;

import org.wso2.carbon.siddhihive.core.configurations.ExecutionPlan;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveService;

import java.util.ArrayList;
import java.util.List;


public class Sample01 {


    private static String streamdef1 = "define stream StockExchangeStream ( symbol string, price int )";
    private static String streamdef2 = "define stream StockQuote ( symbol string, avgPrice double )";
    private static String query1 = " from StockExchangeStream[price >= 20]#window.length(50) " +
            " select symbol, avg(price) as avgPrice " +
            " group by symbol having avgPrice > 50 " +
            " insert into StockQuote;";
    private static String fullName1 = "org_wso2_carbon_kpi_publisher";
    private static String fullName2 = "org_wso2_carbon_kpi_publisher_result";
    private static List<String> defList = new ArrayList<String>();
    private static List<String> nameList = new ArrayList<String>();

    public static void main(String[] args) {
        defList.add(streamdef1);
        defList.add(streamdef2);
        nameList.add(fullName1);
        nameList.add(fullName2);
        SampleHelper sampleHelper = new SampleHelper();
        ExecutionPlan executionPlan = sampleHelper.getExecutionPlan(query1, defList, nameList);
        SiddhiHiveService siddhiHiveService = new SiddhiHiveService();
        String result = siddhiHiveService.addExecutionPlan(executionPlan);
        System.out.println(result);
    }
}
