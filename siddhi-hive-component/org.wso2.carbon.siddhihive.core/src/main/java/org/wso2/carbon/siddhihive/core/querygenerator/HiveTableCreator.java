package org.wso2.carbon.siddhihive.core.querygenerator;


import java.util.ArrayList;
import java.util.List;

import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

public final class HiveTableCreator extends HiveQueryGenerator {
	//**********************************************************************************************
    private String sInsertQuery = "";
    private String sHiveColumns = "";
    private String sCSVQuery = "";
	private String sCassandraQuery = "";
	private String sCassandraProperties = "";
	private String sCassandraColumns = "";
    private String sSQLQuery = "";
    private String sSQLProperties = "";
    private String sSQLColumns = "";

	String sDBName;
	List<HiveField> listColumns;
	
	//**********************************************************************************************
	public HiveTableCreator() {
		super();
    }

    //**********************************************************************************************
    public void setQuery(String sStreamID, String sResultTable, List<String> listFields) {
        sDBName = sResultTable;
        listColumns = new ArrayList<HiveField>();
        for (int i=0; i < listFields.size(); i++) {
            //get the datatype, create HiveField object and fill the list
        }
    }
	
	//**********************************************************************************************
	public void setQuery(StreamDefinition outputStreamDef) {
		sDBName = outputStreamDef.getStreamId();
        List<Attribute> attributeList = outputStreamDef.getAttributeList();
        sDBName = outputStreamDef.getStreamId();
        listColumns = new ArrayList<HiveField>();
        Attribute attribute = null;
        for(int i = 0; i < attributeList.size(); i++) {
            attribute = attributeList.get(i);
            listColumns.add(new HiveField(attribute.getName(), siddhiToHiveType(attribute.getType())));
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
		
		sCSVQuery = ("CREATE TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns + ") " +
				"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + " " +  "STORED AS SEQUENCEFILE" + ";");
		
		return sCSVQuery;
	}
	
	//**********************************************************************************************
	public String getCassandraTableCreateQuery() {		
		if (listColumns.size() <= 0)
			return null;
		
		fillColumnString();
		fillCassandraProperties();
		
		sCassandraQuery = ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns +
				") STORED BY \'org.apache.hadoop.hive.cassandra.CassandraStorageHandler\' WITH SERDEPROPERTIES " +
				"(" + sCassandraProperties +");");
		
		
		return sCassandraQuery;
	}

    //**********************************************************************************************
    public String getSQLTableCreateQuery() {
        if (listColumns.size() <= 0)
            return null;

        fillColumnString();
        fillSQLProperties();

        sSQLQuery = ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns
                +") STORED BY \'org.wso2.carbon.hadoop.hive.jdbc.storage.JDBCStorageHandler\' TBLPROPERTIES ("
                +sSQLProperties+");");

        return  sSQLQuery;
    }
	
	//**********************************************************************************************
	private void fillColumnString() {
		sHiveColumns = listColumns.get(0).getFieldName() + " " + listColumns.get(0).getDataType();
		for (int i = 1; i < listColumns.size(); i++) {
			sHiveColumns += (", " + listColumns.get(i).getFieldName() + " " + listColumns.get(i).getDataType());
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

    //**********************************************************************************************
    private void fillSQLProperties() {
        fillSQLColumnString();

        sSQLProperties = ("\'wso2.carbon.datasource.name\' = \'"+Constants.CARBON_DATASOURCE+"\', "
        +"\'hive.jdbc.table.create.query\' = \'CREATE TABLE "+sDBName+"_summary ("+sSQLColumns+")\'");
    }

    //**********************************************************************************************
    private void fillSQLColumnString() {
        sSQLColumns = (listColumns.get(0).getFieldName() + " " + hiveToSQLType(listColumns.get(0).getDataType()));
        for (int i = 1; i < listColumns.size(); i++) {
            sSQLColumns += (", " + listColumns.get(i).getFieldName() + " " + hiveToSQLType(listColumns.get(i).getDataType()));
        }
    }
}
