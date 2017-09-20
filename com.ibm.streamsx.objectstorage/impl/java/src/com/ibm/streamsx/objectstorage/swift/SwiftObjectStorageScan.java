package com.ibm.streamsx.objectstorage.swift;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.objectstorage.BaseObjectStorageScan;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@PrimitiveOperator(name = "SwiftObjectStorageScan", namespace = "com.ibm.streamsx.objectstorage.swift", description = "Java Operator ObjectScan for Swift")
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating),
		@OutputPortSet(description = "Optional output ports", optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class SwiftObjectStorageScan extends BaseObjectStorageScan implements ISwiftObjectStorageAuth {

	private String fContainer = null;
	private String fObjectStorageServiceName = null;
	private SwiftProtocol fProtocol = SwiftProtocol.swift2d;
	
	@Override
	public void initialize(OperatorContext context) throws Exception {		
		setURI(Utils.getObjectStorageSwiftURI(fProtocol, getServiceName(), getContainer()));
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

	@Parameter
	public void setServiceName(String serviceName) {
		fObjectStorageServiceName = serviceName;
		
	}

	public String getServiceName() {
		return fObjectStorageServiceName;
	}

	@Parameter(optional=true)
	public void setAccessPoint(String objectStorageAccessPoint) {
		super.setEndpoint(objectStorageAccessPoint);
	}
	
	public String getAccessPoint() {
		return super.getEndpoint();
	}
}
