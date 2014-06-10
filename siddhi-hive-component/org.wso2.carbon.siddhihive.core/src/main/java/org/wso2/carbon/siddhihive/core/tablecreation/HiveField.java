package org.wso2.carbon.siddhihive.core.tablecreation;

public final class HiveField {
	//**********************************************************************************************
	private String sFieldName = "";
	private String sDataType = "";
	
	//**********************************************************************************************
	public HiveField (String sName, String sType) {
		sFieldName = sName;
		sDataType = sType;
	}

	//**********************************************************************************************
	public String getFieldName() {
		return sFieldName;
	}

	//**********************************************************************************************
	public String getDataType() {
		return sDataType;
	}	
}
