package org.wso2.carbon.siddhihive.core.querygenerator;

import org.wso2.carbon.siddhihive.core.utils.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;

public abstract class HiveQueryGenerator {
	
	//**********************************************************************************************
	public HiveQueryGenerator () {
		
	}
	
	//**********************************************************************************************
	public String typeToString (Attribute.Type type) {
		switch (type) {
			case STRING:
				return Constants.STRING;
			case INT:
				return Constants.INT;
			case DOUBLE:
				return Constants.DOUBLE;
			default:
				return Constants.VARCHAR;	
		}
	}
}