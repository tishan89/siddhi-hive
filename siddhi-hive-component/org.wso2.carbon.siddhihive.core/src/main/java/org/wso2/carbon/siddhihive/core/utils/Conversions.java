package org.wso2.carbon.siddhihive.core.utils;

import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by prasad on 6/10/14.
 */
public class Conversions {
    //**********************************************************************************************
    public static String siddhiToHiveType(Attribute.Type type) {
        switch (type) {
            case STRING:
                return Constants.H_STRING;
            case INT:
                return Constants.H_INT;
            case DOUBLE:
                return Constants.H_DOUBLE;
            default:
                return Constants.H_BINARY;
        }
    }

    //**********************************************************************************************
    public static String hiveToSQLType(String sType) {
        if (sType.equals(Constants.H_STRING)) {
            return Constants.S_STRING;
        } else if (sType.equals(Constants.H_INT)) {
            return Constants.S_INT;
        } else if (sType.equals(Constants.H_DOUBLE)) {
            return Constants.S_DOUBLE;
        } else {
            return Constants.S_BINARY;
        }
    }
}
