package org.wso2.carbon.siddhihive.core.tablecreation;


public final class CSVTableCreator extends TableCreatorBase {
	//**********************************************************************************************
    private String sCSVQuery = "";

	
	//**********************************************************************************************
	public CSVTableCreator() {
		super();
    }
	
	//**********************************************************************************************
	public String getQuery() {
		if (listColumns.size() <= 0)
			return null;
		
		fillHiveFieldString();

        sCSVQuery = "DROP TABLE IF EXISTS " +  sDBName + " ;\n";

		sCSVQuery += ("CREATE TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns + ") " +
				"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + " " +  "STORED AS SEQUENCEFILE" + ";");

		return sCSVQuery;
	}
}
