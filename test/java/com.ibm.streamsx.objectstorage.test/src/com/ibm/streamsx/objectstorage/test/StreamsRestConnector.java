package com.ibm.streamsx.objectstorage.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.rest.Operator;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

public class StreamsRestConnector {

    StreamsConnection connection;
    String instanceName;
    Instance instance;
    Job job;
    String jobId;
    String testType;

    public StreamsRestConnector() {}

    private String getUserName() {
        // allow the user to specify a different user name for this test
        String userName = System.getenv("STREAMS_INSTANCE_USERID");
        if ((userName == null) || (userName.isEmpty())) {
            userName = System.getenv("USER");
        }
        return userName;
    }

    private String getStreamsPort() {
        String streamsPort = System.getenv("STREAMS_INSTANCE_PORT");
        if ((streamsPort == null) || streamsPort.isEmpty()) {
            // if port not specified, assume default one
            streamsPort = "8443";
        }
        return streamsPort;
    }

    private String getUserPassword() {
        String instancePassword = System.getenv("STREAMS_INSTANCE_PASSWORD");
        // Default password for the QSE
        if ("streamsadmin".equals(getUserName()) && instancePassword == null) {
            instancePassword = "passw0rd";
        }
        // don't print this out unless you need it
        // System.out.println("InstancePWD: " + instancePassword);
        return instancePassword;
    }

    private void setupConnection() throws Exception {
        if (connection == null) {
            testType = "DISTRIBUTED";

            instanceName = System.getenv("STREAMS_INSTANCE_ID");
            System.out.println("InstanceName: " + instanceName);

            String userName = getUserName();
            System.out.println("UserName: " + userName);
            String streamsPort = getStreamsPort();
            System.out.println("streamsPort: " + streamsPort);
            String instancePassword = getUserPassword();

            // if the instance name and password are not set, bail
            assumeNotNull(instanceName, instancePassword);

            String restUrl = "https://localhost:" + streamsPort + "/streams/rest/resources";
            connection = StreamsConnection.createInstance(userName, instancePassword, restUrl);

            // for localhost, need to disable security
            connection.allowInsecureHosts(true);
        }
    }

    private void setupInstance() throws Exception {
        setupConnection();

        if (instance == null) {
            instance = connection.getInstance(instanceName);
            // don't continue if the instance isn't started
            System.out.println("Instance: " + instance.getStatus());
            assumeTrue(instance.getStatus().equals("running"));
        }
    }


    public Job submitJob(Topology topology, StreamsContext.Type testerType) throws Exception {
        setupInstance();
        
        if (jobId == null) {
            long jobSubmissionTime = System.currentTimeMillis();
        	if (testerType.equals(StreamsContext.Type.DISTRIBUTED_TESTER)) {            	
                jobId = StreamsContextFactory.getStreamsContext(StreamsContext.Type.DISTRIBUTED).submit(topology).get()
                        .toString();
            } else if (testType.equals("STREAMING_ANALYTICS_SERVICE")) {
                jobId = StreamsContextFactory.getStreamsContext(StreamsContext.Type.STREAMING_ANALYTICS_SERVICE)
                        .submit(topology).get().toString();
            } else {
                fail("This test should be skipped");
            }

            job = instance.getJob(jobId);
            
            
            job.waitForHealthy(60, TimeUnit.SECONDS);
            System.out.println("Rest reported job healthy in msec: '" + String.valueOf(System.currentTimeMillis() - jobSubmissionTime) + "'");
             
            assertEquals("healthy", job.getHealth());
        }
        System.out.println("jobId: " + jobId + " is setup.");
        
		return job;
    }

    public void removeJob() throws Exception {
        if (job != null) {
        	job.cancel();
            job = null;
        }
    }


    public List<Metric> getMetrics(String operatorKind, int metricsCollectionTimeoutSecs) throws Exception {
    	List<Metric> res = new LinkedList<Metric>();

    	Thread.sleep(metricsCollectionTimeoutSecs * 1000);
    	for (Operator op: job.getOperators()) {
        	if (op.getOperatorKind().equals(operatorKind)) {
        		res.addAll(op.getMetrics());
        	}
        }
        
        return res;
    }

    

}