package org.wso2.carbon.siddhihive.core.querygenerator;

import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;

public abstract class HiveQueryGenerator {
	
	//**********************************************************************************************
	public HiveQueryGenerator () {
		
	}
	
	//**********************************************************************************************
	public String siddhiToHiveType(Attribute.Type type) {
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
    public String hiveToSQLType(String sType) {
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