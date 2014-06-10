package org.wso2.carbon.siddhihive.core.utils;


public final class Constants {
    public final static String TIME_WINDOW = "time";
    public final static String TIME_BATCH_WINDOW = "timeBatch";
    public final static String FROM_TIME = "fromTime";
    public final static String TO_TIME = "toTime";
    public final static String INCREMENTAL_KEYWORD = "@incremental";
    public static final String NAME = "name";
    public static final String TABLE_REFERENCE = "tables";
    public static final String HAS_NON_INDEX_DATA = "hasNonIndexedData";
    public static final String BUFFER_TIME = "bufferTime";

    public static final String FROM = "from";
    public static final String WHERE = "where";

    public static final String FROM_CLAUSE = "fromClause";
    public static final String WHERE_CLAUSE = "whereClause";
    public static final String INCREMENTAL_CLAUSE = "incremental";

    public static final String TIME_WINDOW_FREQUENCY = "timeWindowFrequency";

    public static final String META = "meta";
    public static final String CORRELATION = "correlation";

    //**********************************************************************************************
    public static final String H_STRING = "STRING";
    public static final String H_INT = "INT";
    public static final String H_DOUBLE = "DOUBLE";
    public static final String H_BINARY = "BINARY";
    public static final String S_STRING = "VARCHAR(128)";
    public static final String S_INT = "INT";
    public static final String S_DOUBLE = "DOUBLE(16,4)";
    public static final String S_BINARY = "BLOB";
    public static final String CASSANDRA_DATASOURCE = "WSO2BAM_CASSANDRA_DATASOURCE";
    public static final String CARBON_DATASOURCE = "WSO2BAM_DATASOURCE";

    public static final String AND = "AND";
    public static final String OR = "OR";

    public static final String HAVING = " having ";
    public static final String OPENING_BRACT = " ( ";
    public static final String CLOSING_BRACT = " ) ";
    public static final String SPACE = "  ";

    public static final String SELECTION_QUERY = "selectionQuery";
    public static final String GROUP_BY_QUERY = "groupByQuery";
    public static final String HAVING_QUERY = "havingQuery";

    public static final String DEFAULT_SLIDING_FREQUENCY = "600";
    public static final String LENGTH_WINDOW = "length";
    public static final String LENGTH_BATCH_WINDOW = "lengthBatch";
}
