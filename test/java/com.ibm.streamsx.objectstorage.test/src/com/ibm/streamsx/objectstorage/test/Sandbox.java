package com.ibm.streamsx.objectstorage.test;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.NameFileComparator;

public class Sandbox {

	public static List<File> getAllFilesInFolder(String path) {
		List<File> res = ((List<File>) FileUtils.listFiles(new File(path), null, true));
		//res.sort(NameFileComparator.NAME_INSENSITIVE_REVERSE);		
		//res.sort(NameFileComparator.NAME_COMPARATOR);
		//res.sort(NameFileComparator.NAME_SYSTEM_COMPARATOR);
		res.sort(NameFileComparator.NAME_SYSTEM_REVERSE);

		
		return res;
	}
	public static void main(String[] args) {
		String path = "/home/streamsadmin/git-public/streamsx.objectstorage/test/java/com.ibm.streamsx.objectstorage.test/data/expected/com.ibm.streamsx.objectstorage.unitest.sink.parquet.partitioning.TestCloseBySizeParquetAutoPartitioning";
		List<File> files = getAllFilesInFolder(path);
		int fCount = files.size();
		System.out.println(fCount + " files found in folder '" + path + "'");
		for (File f: files) {
			System.out.println(f.getName());
		}
		
	}
	
	

}
