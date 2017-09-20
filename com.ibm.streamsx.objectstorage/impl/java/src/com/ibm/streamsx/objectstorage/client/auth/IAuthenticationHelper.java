package com.ibm.streamsx.objectstorage.client.auth;

/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/


import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

public interface IAuthenticationHelper {

	public FileSystem connect(String fileSystemUri, final String hdfsUser,
			Map<String, String> connectionProperties) throws Exception;

	public void disconnect();

}
