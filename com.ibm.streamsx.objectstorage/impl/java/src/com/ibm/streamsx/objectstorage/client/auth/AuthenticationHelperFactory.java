/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.objectstorage.client.auth;

public class AuthenticationHelperFactory {

	public static IAuthenticationHelper createAuthenticationHelper(
			String hdfsUri, String hdfsUser, String configPath)
			throws Exception {
		IAuthenticationHelper authHelper = null;

		// if no URI is defined by the user, default to HDFS
		if (hdfsUri == null) {
			authHelper = new OSAuthenticationHelper(configPath);
		} else {
			authHelper = new OSAuthenticationHelper(configPath);
		}

		return authHelper;
	}
}
