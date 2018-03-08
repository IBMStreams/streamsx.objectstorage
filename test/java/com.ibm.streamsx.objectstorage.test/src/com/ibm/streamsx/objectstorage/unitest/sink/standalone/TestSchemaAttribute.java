package com.ibm.streamsx.objectstorage.unitest.sink.standalone;

import com.ibm.streams.operator.Type.MetaType;

public class TestSchemaAttribute {
	
	private String name; 
	private String value;
	private MetaType type;
	
	
	public TestSchemaAttribute(String name, String value, MetaType type) {
		this.name = name;
		this.value = value;
		this.type = type;
	}
	
	public MetaType getType() {
		return type;
	}
	
	public String getValue() {
		return value;
	}
	
	public String getName() {
		return name;
	}
	
}
