package com.ibm.streamsx.objectstorage.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.NameFileComparator;

public interface OSTFileUtils {

	
	public List<String> readFileLineByLine(String inFile) throws IOException;
	public void showFileDiffs(File file1, File file2) throws Exception;
	public boolean contentContains(File file1, File file2) throws IOException;
	public boolean contentEquals(File file1, File file2) throws IOException;
	
	
	public default boolean fileExists(String  filePath) {
		return new File(filePath).exists();
	}
	    
		
	public default boolean folderExists(String path) {
		return new File(path).exists();
	}
	
	public default void createFolder(String path) {
		new File(path).mkdir();
	}


	public default void removeFolder(String path)  {
		try {
			FileUtils.deleteDirectory(new File(path));
		} catch (IOException ioe) {
			System.out.println("Failed to remove folder '" + path + "'");
		}
	}
	
	public default List<File> getAllFilesInFolder(String path) {
		List<File> res = ((List<File>) FileUtils.listFiles(new File(path), null, true));
		res.sort(NameFileComparator.NAME_INSENSITIVE_REVERSE);
		
		return res;
	}
	
	public default HashMap<String, File> getFilesInFolder(String path, String extension) {
		HashMap<String, File> res = new HashMap<String, File>();
		String[] extensions = extension != null ? new String[] { extension } : null;
		List<File> files = (List<File>)  FileUtils.listFiles(new File(path), extensions, true);		
		for (File file: files) {
			res.put(file.getName(), file);
		}
		
		return res;
	}

}
