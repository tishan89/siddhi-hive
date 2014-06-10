package org.wso2.carbon.siddhihive.core.samples;

import java.util.ArrayList;
import java.util.List;

import org.wso2.carbon.siddhihive.core.tablecreation.*;
import org.wso2.carbon.siddhihive.core.utils.Constants;

public class TestQueryGenerator {

	public static void main(String[] args) {
        // TODO Auto-generated method stub

        List<HiveField> map = new ArrayList<HiveField>();
        map.add(new HiveField("col1", Constants.H_STRING));
        map.add(new HiveField("col2", Constants.H_INT));
        map.add(new HiveField("col3", Constants.H_DOUBLE));
        map.add(new HiveField("col4", Constants.H_INT));

        TableCreatorBase a = new CSVTableCreator();
        a.setQuery("mydb", map, "wso2.org.carbon.hive.mydb");
        System.out.println(a.getInsertQuery());
        System.out.println(a.getQuery());

        a = new SQLTableCreator();
        a.setQuery("mydb", map, "wso2.org.carbon.hive.mydb");
        System.out.println(a.getInsertQuery());
        System.out.println(a.getQuery());

        a = new CassandraTableCreator();
        a.setQuery("mydb", map, "wso2.org.carbon.hive.mydb");
        System.out.println(a.getInsertQuery());
        System.out.println(a.getQuery());

        map.clear();
	}
}
