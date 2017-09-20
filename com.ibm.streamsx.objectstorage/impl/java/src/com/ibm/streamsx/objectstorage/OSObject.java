/*******************************************************************************
* Copyright (C) 2017, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/
package com.ibm.streamsx.objectstorage;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;
import com.ibm.streamsx.objectstorage.writer.IWriter;
import com.ibm.streamsx.objectstorage.writer.WriterFactory;
import com.ibm.streamsx.objectstorage.writer.WriterType;
import com.ibm.streamsx.objectstorage.writer.raw.RawAsyncWriter;


enum EnumFileExpirationPolicy {
	NEVER, SIZE, TUPLECNT, PUNC, TIME
}

public class OSObject {

	private IObjectStorageClient fObjectStorageClient;
	private String fPath;
	private String fHeader;
	//private AsyncBufferWriter fWriter;
	private IWriter fWriter;

	private boolean fIsExpired;
	private EnumFileExpirationPolicy expPolicy = EnumFileExpirationPolicy.NEVER;
	
	private static final String UTF_8 = "UTF-8";

	// punctuation will simply set the file as expired
	// and not tracked by the file.
	private long size;
	private long sizePerObject ;

	private long tupleCnt;
	private long tuplesPerFile;

	@SuppressWarnings("unused")
	private double timePerFile;
	private String fEncoding = UTF_8;
	
	int numTuples = 0;
	private byte[] fNewLine;

	/// The metatype of the attribute we'll be working with.
	private final MetaType fDataAttrType;
	
	/// The index of the attribute that matters.
	private final int fDataAttrIndex;
	
	private OperatorContext fOpContext;
	
	private String partitionPath;
	
	private boolean isAppend = false; 	// default is false, overwrite file
	
	private StorageFormat fStorageFormat = StorageFormat.parquet;
	private Thread fObjectTimerThread;
	
	private static final String CLASS_NAME = "com.ibm.streamsx.objectstorage"; 
	
	private static Logger TRACE = Logger.getLogger(OSObject.class.getName());

	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);


	/**
	 * Create an instance of OSObject
	 * @param context	Operator context.
	 * @param path		name of the file
	 * @param client	hdfs connection
	 * @param dataEncoding	The file encoding; only matters for text files.
	 * @param dataAttrIndex	The index of the attribute we'll be writing.
	 * @param dataAttrType	The index of the attribute we'll be writing.
	 */
	public OSObject(final OperatorContext context,			   
					final String path, 
				    final String header, 
				    final IObjectStorageClient client, 
				    String dataEncoding,
				    final int dataAttrIndex, 
				    final MetaType dataAttrType,
				    final String storageFormat) {
		
		TRACE.log(TraceLevel.DEBUG, "Initializing OSObject with path '"  + path + "' and storage format '" + storageFormat + "'");
		
		fPath = path;
		fHeader = header;
		fObjectStorageClient = client;
		fOpContext = context;
		
		if (dataEncoding == null)
		{
			dataEncoding = UTF_8;
		}
		
		fEncoding = dataEncoding;

		try {
			fNewLine = System.getProperty("line.separator").getBytes(fEncoding);
		} catch (UnsupportedEncodingException e) {
			fNewLine = System.getProperty("line.separator").getBytes();
		}
		fDataAttrIndex = dataAttrIndex;
		fDataAttrType = dataAttrType;
		fStorageFormat = StorageFormat.valueOf(storageFormat);
	}

	
	public void writeTuple(Tuple tuple) throws Exception {
		if (fWriter == null) {
			initWriter(tuple, fDataAttrIndex, fDataAttrType, fNewLine);
		}

		// take measure of current data 
		// size in buffer before writing
		long prevDataSize = fWriter.getDataSize();
		
		fWriter.write(tuple, fDataAttrIndex, fDataAttrType, fEncoding);		
		
		// update current data size AFTER writing
		size = fWriter.getDataSize();
		long tupleSize = size - prevDataSize;
		
		// increase tuple counter
		numTuples++;
		
		// check expiration after write, so the next write
		// will create a new object			
		if (expPolicy != EnumFileExpirationPolicy.NEVER)
		{
			switch (expPolicy) {
			case TUPLECNT:
				tupleCnt++;
				if (tupleCnt >= tuplesPerFile) {
					setExpired();
				}
				break;
			case SIZE:
				if((sizePerObject - size) < tupleSize)
				{
					setExpired();
				}
				break;	
			default:
				break;
			}
		}
	}


	public void setExpired() {
		fIsExpired = true;
	}

	public boolean isExpired() {
		return fIsExpired;
	}

	public IObjectStorageClient getObjectStoreClient() {
		return fObjectStorageClient;
	}
	
	/**
	 * This method returns the size of the object as tracked by the operator.
	 * When data is written to Object Storage, if data is not flush and sync'ed with the object storage
	 * the objectStorageClient.getObjectSize method will not reflect the latest size of the object.
	 * The operator keeps track of how much data has been written to the object storage,
	 * and use this information to determine when a object should be closed and a new object should
	 * be created
	 * @return the size of object as tracked by operator
	 */
	public long getSize() {
		
		return size;
	}
	
	/**
	 * This method goes to the object storage client and returns the size of an object as known
	 * by the object storage.  If any error occurs when fetching the size of the object,
	 * the method returns the last known size by the operator.  This method should NOT
	 * be used to determine the current size of the object, if the object is being written to
	 * and data is buffered.  This method is created for reporting purposes when the operator
	 * needs to return the actual size of the object from the object storage.
	 * @return size of object as known by object storage.
	 */
	public long getSizeFromObjectStore()
	{
		try {
			return fObjectStorageClient.getObjectSize(fPath);
		} catch (IOException e) {
			
		}
		return getSize();
	}
	
	// can only be called by HDFS2FileSink on reset
	void setSize(long size) {
		this.size = size;
	}
	
	public long getTupleCnt() {
		return tupleCnt;
	}

	// can only be called by HDFS2FileSink on reset
	void setTupleCnt(long tupleCnt) {
		this.tupleCnt = tupleCnt;
	}

	/**
	 * Init the writer.
	 * Only one thread can create a new writer.  Write can be created by init, write or flush.  The synchronized keyword prevents
	 * write and flush to create writer at the same time. 
	 * @param dataAttrIndex 
	 * @param dataAttrType 
	 * @param newLine 
	 * @param isBinary  If true, file is considered a binary file.  If not, it is assumed to be a text file, and a newline is added after each write.
	 * @throws IOException
	 * @throws Exception
	 */
	//synchronized private void initWriter(boolean isBinary, boolean append) throws IOException, Exception {
	synchronized private void initWriter(Tuple tuple, int dataAttrIndex, MetaType dataAttrType, byte[] newLine) throws IOException, Exception {
		
		if (fWriter == null)
		{
			
			// generates writer (raw or parquet) according to the
			// given settings
			fWriter = WriterFactory.getInstance().getWriter(fPath, 
														    fOpContext,  
														    tuple,
														    dataAttrIndex, 
														    dataAttrType,
														    getObjectStoreClient(), 
														    fStorageFormat,
														    newLine);
			
			if (fHeader != null) {								
				fWriter.write(fHeader.getBytes(fEncoding));
			}
		}
	}

	public void close() throws Exception {

		// stop the timer thread.. and create a new one when a new file is
		// created.
		if (fObjectTimerThread != null) {
			// interrupt the thread if we are not on the same thread
			// otherwise, keep it going...
			if (Thread.currentThread() != fObjectTimerThread) {
				TRACE.log(TraceLevel.DEBUG, "Stop object timer thread"); 
				fObjectTimerThread.interrupt();
			}

			fObjectTimerThread = null;
		}
		
		if (fWriter != null) {
			fWriter.close();
		}

		// do not close output stream, rely on the writer to close

	}

	public EnumFileExpirationPolicy getExpPolicy() {
		return expPolicy;
	}

	public void setExpPolicy(EnumFileExpirationPolicy expPolicy) {
		this.expPolicy = expPolicy;
	}

	public void setSizePerObject(long sizePerFile) {
		this.sizePerObject  = sizePerFile;
	}

	/**
	 * Time before the file expire, timePerfile is expected to be in 
	 * miliseconds
	 * @param timePerFile  Time before the file expire, timePerfile is expected to be in 
	 */
	public void setTimePerObject(double timePerFile) {
		this.timePerFile = timePerFile;
	}

	public void setTuplesPerObject(long tuplesPerFile) {
		this.tuplesPerFile = tuplesPerFile;
	}
	
	public void setAppend(boolean append) {
		this.isAppend = append;
	}
	
	public void setPartitionPath(String path) {
		this.partitionPath = path;
	}
	
	public String getPartitionPath() {
		return this.partitionPath;
	}
	
	public boolean isAppend() {
		return isAppend;
	}
	
	public String getPath() {
		return fPath;
	}
	
	// called by drain method for consistent region
	public void flush() throws Exception {
		// close the current writer and recreate
		
		if (fWriter != null)
		{
			fWriter.flushAll();
		}
		
	}
	
	public boolean isClosed()
	{
		if (fWriter != null)
			return fWriter.isClosed();
		
		return true;
	}
	
	public void createObjectTimer(final double time) {

		// This thread handles the timePerFile expiration policy
		// This thread sleeps for the time specified.
		// When it wakes up, it will set the file as expired and close it
		// When the next tuple comes in, we check that the file has
		// expired and will create a new file for writing
		fObjectTimerThread = fOpContext.getThreadFactory().newThread(
				new Runnable() {

					@Override
					public void run() {
						try {
							TRACE.log(TraceLevel.DEBUG, "Object timer for object '" + getPath() + "' has been initialized for '" + time + "' ms");
							Thread.sleep((long) time);
							TRACE.log(TraceLevel.DEBUG, "Object timer for object '" + getPath() + "' has expired, close object"); 
							setExpired();
							close();
						} catch (Exception e) {
							TRACE.log(TraceLevel.DEBUG,
									"Exception in file timer thread.", e); 
						}
					}
				});
		fObjectTimerThread.setDaemon(false);
		fObjectTimerThread.start();
	}

}
