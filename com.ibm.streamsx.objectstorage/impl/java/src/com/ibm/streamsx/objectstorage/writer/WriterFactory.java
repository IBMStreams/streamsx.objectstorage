package com.ibm.streamsx.objectstorage.writer;

import org.apache.hadoop.fs.Path;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;
import com.ibm.streamsx.objectstorage.internal.sink.StorageFormat;
import com.ibm.streamsx.objectstorage.writer.parquet.ParquetOSWriter;
import com.ibm.streamsx.objectstorage.writer.parquet.ParquetWriterConfig;
import com.ibm.streamsx.objectstorage.writer.raw.RawAsyncWriter;
import com.ibm.streamsx.objectstorage.writer.raw.RawSyncWriter;

public class WriterFactory {

	private static final String CLASS_NAME = WriterFactory.class.getName();
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);
	
	private static WriterFactory instance = null;

	
	public synchronized static WriterFactory getInstance() {
		if (instance == null) {
			instance = new WriterFactory();
		}

		return instance;
	}

	public IWriter getWriter(String path, 
			 OperatorContext opContext,
			 int dataAttrIndex,		 
			 IObjectStorageClient storageClient, 
			 StorageFormat fStorageFormat, 
			 byte[] newLine,
			 final String parquetSchemaStr,
			 ParquetWriterConfig parquetWriterConfig) throws Exception {
		
		IWriter res = null;
		
		boolean isBlob = dataAttrIndex >=0 ? com.ibm.streamsx.objectstorage.Utils.getAttrMetaType(opContext, dataAttrIndex) == MetaType.BLOB : false;
		
		switch (fStorageFormat) {
		case raw:
			if (TRACE.isLoggable(TraceLevel.TRACE)) {
				TRACE.log(TraceLevel.TRACE, "Creating raw sync writer for object with  path '" + path + "'");
			}

			//res = new RawAsyncWriter(path, opContext, storageClient, isBlob ? new byte[0] : newLine);
			res = new RawSyncWriter(path, storageClient, isBlob ? new byte[0] : newLine);
			break;

		case parquet:
			if (TRACE.isLoggable(TraceLevel.TRACE)) {
				TRACE.log(TraceLevel.TRACE,
						"Creating parquet writer for object with parent path '"
								+ storageClient.getObjectStorageURI()
								+ "' and child path '" + path + "'");
			}

			res = new ParquetOSWriter(
					new Path(storageClient.getObjectStorageURI() + path),
					parquetSchemaStr,
					storageClient.getConnectionConfiguration(), parquetWriterConfig);

			break;

		default:
			if (TRACE.isLoggable(TraceLevel.TRACE)) {
				TRACE.log(TraceLevel.TRACE, "Creating raw async writer for object with  path '" + path + "'");
			}

			res = new RawAsyncWriter(path,
					opContext,
					storageClient,
					isBlob ? new byte[0] : newLine);
			break;
		}

		return res;
	}

}
