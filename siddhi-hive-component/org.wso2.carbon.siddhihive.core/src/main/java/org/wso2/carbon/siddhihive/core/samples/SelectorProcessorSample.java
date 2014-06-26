package org.wso2.carbon.siddhihive.core.samples;

import org.wso2.carbon.siddhihive.core.configurations.StreamDefinitionExt;
import org.wso2.carbon.siddhihive.core.internal.SiddhiHiveManager;
import org.wso2.carbon.siddhihive.core.selectorprocessor.QuerySelectorProcessor;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.query.Query;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by firzhan on 6/1/14.
 */
public class SelectorProcessorSample {

    public static void main(String[] args) {



        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream StockExchangeStream ( symbol string, price int )");
        siddhiManager.defineStream("define stream FastMovingStockQuotes ( symbol string, avgPrice double )");
//        String queryID = siddhiManager.addQuery(" from StockExchangeStream[price >= 20]#window.time(50) " +
//                                                " select symbol, avg(price) as avgPrice " +
//                " group by symbol having avgPrice > 50 " +
//                " insert into StockQuote;");

//        Query query = siddhiManager.getQuery(queryID);
//
        SiddhiHiveManager siddhiHiveManager = new SiddhiHiveManager();

        int count = 0;
        while(count < 2){
        QuerySelectorProcessor selectorProcessor = new QuerySelectorProcessor();
//        HashMap<String, String> map = (HashMap<String, String>)selectorProcessor.handleSelector(query);
//
//        for (Object value : map.values()) {
//
//            System.out.println(value.toString());
//        }

//        from TickEvent[symbol==’IBM’]#window.length(2000) join
//        NewsEvent#window.time(500)
//        select *
//                insert into JoinStream

//        String queryID = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"IBM\"]#window.length(1)" +
//        "select symbol,price, avg(price) as averagePrice \n" +
//                "group by symbol, price  \n" +
//                "having ((price > averagePrice*1.02) and (averagePrice*0.98 > price ))\n" +
//                "insert into FastMovingStockQuotes");

        System.out.println("+++++++++++++++++++++++++++");
//        String queryID = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"IBM\"]#window.lengthBatch(1)\n" +
//                "select symbol,price, avg(price) as averagePrice \n" +
//                "group by symbol\n" +
//                "having ((price > averagePrice*1.02) and ( (averagePrice*0.98 > price) or (averagePrice*0.98 < price) ))\n" +
//                "insert into FastMovingStockQuotes;");



           String queryID = siddhiManager.addQuery(" from StockExchangeStream[symbol == \"Apple\"]#window.length(10) \n" +
                "join FastMovingStockQuotes#window.lengthBatch(15)  \n" +
                " on StockExchangeStream.symbol == FastMovingStockQuotes.symbol  select *\n" +
                "insert into JoinStream;");


        Query query = siddhiManager.getQuery(queryID);


        List<StreamDefinition> streamDefinitionList = siddhiManager.getStreamDefinitions();
        List<StreamDefinitionExt> streamDefinitionExtList = new ArrayList<StreamDefinitionExt>() ;

        for (int i = 0; i < streamDefinitionList.size(); ++i) {

            StreamDefinition streamDefinition = streamDefinitionList.get(i);
            StreamDefinitionExt streamDefinitionExt = new StreamDefinitionExt(streamDefinition.getStreamId(), streamDefinition);

            streamDefinitionExtList.add(streamDefinitionExt);
           // siddhiHiveManager.setStreamDefinition(streamDefinition.getStreamId(), streamDefinition);
        }

        siddhiHiveManager.setStreamDefinition(streamDefinitionExtList);

        String hiveQuery = siddhiHiveManager.getQuery(query);
        System.out.println(hiveQuery);
//
//        try {
//            Thread.sleep(1000);
//        }catch (Exception ex){
//
//        }

            ++count;
       }

//        ConcurrentHashMap<String, String> map = null;
//
//        if (selectorProcessor.handleSelector(query)) {
//            map = (ConcurrentHashMap<String, String>) selectorProcessor.getSelectorQueryMap();
//        }
//
//        for (Object value : map.values()) {
//
//            System.out.println(value.toString());
//        }

    }
}
