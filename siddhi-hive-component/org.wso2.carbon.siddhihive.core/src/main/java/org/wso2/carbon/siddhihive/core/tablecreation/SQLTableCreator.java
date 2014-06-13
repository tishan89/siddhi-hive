package org.wso2.carbon.siddhihive.core.tablecreation;

import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.carbon.siddhihive.core.utils.Conversions;

/**
 * Created by prasad on 6/10/14.
 */
public final class SQLTableCreator extends TableCreatorBase{
    //**********************************************************************************************
    private String sSQLQuery = "";
    private String sSQLProperties = "";
    private String sSQLColumns = "";

    //**********************************************************************************************
    public SQLTableCreator() {
        super();
    }

    //**********************************************************************************************
    public String getQuery() {
        if (listColumns.size() <= 0)
            return null;

        fillHiveFieldString();
        fillSQLProperties();

        sSQLQuery = ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns
                +") STORED BY \'org.wso2.carbon.hadoop.hive.jdbc.storage.JDBCStorageHandler\' TBLPROPERTIES ("
                +sSQLProperties+");");

        return  sSQLQuery;
    }

    //**********************************************************************************************
    private void fillSQLProperties() {
        fillSQLColumnString();

        sSQLProperties = ("\'wso2.carbon.datasource.name\' = \'"+ Constants.CARBON_DATASOURCE+"\', "
                +"\'hive.jdbc.table.create.query\' = \'CREATE TABLE "+sDBName+"_summary ("+sSQLColumns+")\'");
    }

    //**********************************************************************************************
    private void fillSQLColumnString() {
        sSQLColumns = (listColumns.get(0).getFieldName() + " " + Conversions.hiveToSQLType(listColumns.get(0).getDataType()));
        for (int i = 1; i < listColumns.size(); i++) {
            sSQLColumns += (", " + listColumns.get(i).getFieldName() + " " + Conversions.hiveToSQLType(listColumns.get(i).getDataType()));
        }
    }
}
