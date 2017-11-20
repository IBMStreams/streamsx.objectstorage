package com.ibm.streamsx.objectstorage.swift;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSource;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;


@PrimitiveOperator(name="SwiftObjectStorageSource", namespace="com.ibm.streamsx.objectstorage.swift",
description="Java Operator ObjectSource for Swift")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class SwiftObjectStorageSource extends BaseObjectStorageSource  implements ISwiftObjectStorageAuth {

	private String fContainer = null;
	private SwiftProtocol fProtocol = SwiftProtocol.swift2d;
	
	@Override
	public void initialize(OperatorContext context) throws Exception {		
		setURI(Utils.getObjectStorageSwiftURI(fProtocol, getContainer()));
		setEndpoint((getAccessPoint() == null) ? Constants.SWIFT_DEFAULT_ENDPOINT : getAccessPoint());
		super.initialize(context);
	}

	@Parameter
	public void setUserID(String objectStorageUser) {
		super.setUserID(objectStorageUser);
	}

	@Parameter	
	public void setPassword(String objectStoragePassword) {
		super.setPassword(objectStoragePassword);
	}

	@Parameter	
	public void setProjectID(String objectStorageProjectID) {
		super.setProjectID(objectStorageProjectID);
	}
	
	@Parameter
	public void setContainer(String container) {
		fContainer = container;
	}

	public String getContainer() {
		return fContainer;
	}

	@Parameter(optional=true)
	public void setAccessPoint(String objectStorageAccessPoint) {
		super.setEndpoint(objectStorageAccessPoint);
	}
	
	public String getAccessPoint() {
		return super.getEndpoint();
	}

}
