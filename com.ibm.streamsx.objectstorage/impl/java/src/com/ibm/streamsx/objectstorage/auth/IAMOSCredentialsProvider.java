package com.ibm.streamsx.objectstorage.auth;


import org.apache.hadoop.conf.Configuration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
//import com.amazonaws.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.streamsx.objectstorage.client.Constants;



/**
 * Credentials provider for IAM authentication
 */
public class IAMOSCredentialsProvider implements AWSCredentialsProvider  {

	AWSCredentials fCredentials;
	
	public IAMOSCredentialsProvider(Configuration conf) {
		// extract credentials from the operator context
		String apiKey = conf.get(Constants.OST_IAM_APIKEY_CONFIG_NAME);
		String serviceInstanceId = conf.get(Constants.OST_IAM_INSTANCE_ID_CONFIG_NAME);
		String tokenEndpoint = conf.get(Constants.OST_IAM_TOKEN_ENDPOINT_CONFIG_NAME);
    	
		// initialize BMX IAM credentials object
		SDKGlobalConfiguration.IAM_ENDPOINT = tokenEndpoint;
        fCredentials = new BasicIBMOAuthCredentials(apiKey, serviceInstanceId);        
	}
	
	@Override
	public com.amazonaws.auth.AWSCredentials getCredentials() {

		return (com.amazonaws.auth.AWSCredentials)fCredentials;
	}

	@Override
	public void refresh() {}

}
