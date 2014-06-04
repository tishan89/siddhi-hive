package org.wso2.carbon.siddhihive.core.querygenerator;


import java.util.ArrayList;
import java.util.List;

import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

public final class HiveTableCreator extends HiveQueryGenerator {
	//**********************************************************************************************
	private String sCreateQuery = "";
	private String sInsertQuery = "";
	private String sCassandraQuery = "";
	private String sColumns = "";
	private String sCassandraProperties = "";
	private String sCassandraColumns = "";

	String sDBName;
	List<HiveField> listColumns;
	
	//**********************************************************************************************
	public HiveTableCreator() {
		super();
    }
	
	//**********************************************************************************************
	public void setQuery(StreamDefinition streamDefinition) {
		sDBName = streamDefinition.getStreamId();
        List<Attribute> attributeList = streamDefinition.getAttributeList();
        sDBName = streamDefinition.getStreamId();
        listColumns = new ArrayList<HiveField>();
        Attribute attribute = null;
        for(int i = 0; i < attributeList.size(); i++) {
            attribute = attributeList.get(i);
            listColumns.add(new HiveField(attribute.getName(), typeToString(attribute.getType())));
        }
	}
	
	//**********************************************************************************************
	public void setQuery(String sDB, List<HiveField> listFields) {
		sDBName = sDB;
		listColumns = listFields;
	}
	
	//**********************************************************************************************
	public String getInsertQuery() {
		sInsertQuery = "";
		
		if (sDBName.length() <= 0) 
			return null;
		
		sInsertQuery = "INSERT OVERWRITE TABLE " + sDBName + " ";
		
		return sInsertQuery;
	}
	
	//**********************************************************************************************
	public String getCSVTableCreateQuery() {		
		if (listColumns.size() <= 0)
			return null;
		
		fillColumnString();
		
		sCreateQuery = ("CREATE TABLE IF NOT EXISTS " + sDBName + " (" + sColumns + ") " + 
				"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + " " +  "STORED AS SEQUENCEFILE" + ";");
		
		return sCreateQuery;
	}
	
	//**********************************************************************************************
	public String getCassandraTableCreateQuery() {		
		if (listColumns.size() <= 0)
			return null;
		
		fillColumnString();
		fillCassandraProperties();
		
		sCassandraQuery = ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sColumns +
				") STORED BY \'org.apache.hadoop.hive.cassandra.CassandraStorageHandler\' WITH SERDEPROPERTIES " +
				"(" + sCassandraProperties +");");
		
		
		return sCassandraQuery;
	}
	
	//**********************************************************************************************
	private void fillColumnString() {
		sColumns = listColumns.get(0).getFieldName() + " " + listColumns.get(0).getDataType();
		for (int i = 1; i < listColumns.size(); i++) {
			sColumns += (", " + listColumns.get(i).getFieldName() + " " + listColumns.get(i).getDataType());
		}
	}
	
	//**********************************************************************************************
	private void fillCassandraProperties() {
		fillCassandraColumnString();
		
		sCassandraProperties = ("\"wso2.carbon.datasource.name\" = \""+Constants.CASSANDRA_DATASOURCE+"\", "
				+"\"cassandra.columns.mapping\" = \""+sCassandraColumns+"\"");
	}
	
	//**********************************************************************************************
	private void fillCassandraColumnString() {
		sCassandraColumns = ":key";
		for (int i=0; i<listColumns.size(); i++) {
			sCassandraColumns += (", payload_" + listColumns.get(i).getFieldName());
		}
	}
}
