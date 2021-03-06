package org.wso2.carbon.siddhihive.core.tablecreation;

import org.wso2.carbon.siddhihive.core.utils.Constants;

import java.util.regex.Pattern;

/**
 * Created by prasad on 6/10/14.
 */
public final class CassandraTableCreator extends TableCreatorBase {
    //**********************************************************************************************
    private String sCassandraQuery = "";
    private String sCassandraProperties = "";
    private String sCassandraColumns = "";

    //**********************************************************************************************
    public CassandraTableCreator() {
        super();
    }

    //**********************************************************************************************
    public String getQuery() {
        if (listColumns.size() <= 0)
            return null;

        fillHiveFieldString();
        fillCassandraProperties();

        sCassandraQuery = "DROP TABLE IF EXISTS " +  sDBName + " ;\n";

        sCassandraQuery += ("CREATE EXTERNAL TABLE IF NOT EXISTS " + sDBName + " (" + sHiveColumns +
                ") STORED BY \'org.apache.hadoop.hive.cassandra.CassandraStorageHandler\' WITH SERDEPROPERTIES " +
                "(" + sCassandraProperties +");");


        return sCassandraQuery;
    }

    //**********************************************************************************************
    private void fillCassandraProperties() {
        fillCassandraColumnString();
        String[] streamID = sFullStreamID.split(":");
        sCassandraProperties = ("\"wso2.carbon.datasource.name\" = \""+ Constants.CASSANDRA_DATASOURCE+"\", "
                + "\"cassandra.cf.name\" = \"" + streamID[0].replaceAll(Pattern.quote("."), "_") + "\", "
                + "\"cassandra.columns.mapping\" = \""+sCassandraColumns+"\"");
    }

    //**********************************************************************************************
    private void fillCassandraColumnString() {
        sCassandraColumns = ":key";
        for (int i=0; i<listColumns.size(); i++) {
            sCassandraColumns += (", payload_" + listColumns.get(i).getFieldName());
        }

        sCassandraColumns += (", Timestamp");
    }
}
