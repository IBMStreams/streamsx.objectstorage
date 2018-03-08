package com.ibm.streamsx.objectstorage.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;


public class OSTRawFileUtils implements OSTFileUtils {
	  
	private static final OSTRawFileUtils instance = new OSTRawFileUtils();
	
	private OSTRawFileUtils() {}
	
	public static OSTRawFileUtils getInstance() {
		return instance;
	}
	
	/**
	 * Reads file line by line
	 */
	public  List<String> readFileLineByLine(String inFile) throws IOException {
		FileReader fin = null;
		BufferedReader bin = null;
		List<String> res = new ArrayList<String>();
		try {
			fin = new FileReader(inFile);
			bin = new BufferedReader(fin);			
			String b;
			while ((b = bin.readLine()) != null) {
				res.add(b);
			}
		} finally {
			if (bin != null) bin.close();
			if (fin != null) fin.close();
		}
		
		return res;
	}

		
	 public void showFileDiffs(File file1, File file2) throws Exception {
		 BufferedReader file1BR = null, file2BR = null;
		 try {
			 file1BR = new BufferedReader(new FileReader(file1));
	         file2BR = new BufferedReader(new FileReader(file2));
	
	         List<String> file1Content = new ArrayList<String>();
	         List<String> file2Content = new ArrayList<String>();
	         String currStr = null;
	         while ((currStr = file1BR.readLine()) != null) {
	        	 file1Content.add(currStr);
	         }
	         while ((currStr = file2BR.readLine()) != null) {
	        	 file2Content.add(currStr);
	         }
	         List<String> deltaList = new ArrayList<String>(file1Content);
	         deltaList.removeAll(file2Content);
	         
	         System.out.println("Exists in '" + file1.getPath() + "', but missing in '" + file2.getPath() + "'");
	         for(int i=0; i < deltaList.size();i++){
	             System.out.println( deltaList.get(i) ); 
	         }
	
	         System.out.println("Exists in '" + file2.getPath() + "', but missing in '" + file1.getPath() + "'");
	         file2Content.removeAll(file1Content);
	         for(int i=0;i < file2Content.size(); i++){
	             System.out.println(file2Content.get(i)); 
	         }
	     } finally {
	    	 if (file1BR != null) file1BR.close();
	    	 if (file2BR != null) file2BR.close();
	     }
         
     }

	/**
	 * Checks if file2 contains all lines from file1 
	 * @param file1
	 * @param file2
	 * @param storageFormat 
	 * @return
	 * @throws IOException
	 */
	public boolean contentContains(File file1, File file2) throws IOException {
		
		List<String> file1Content = FileUtils.readLines(file1, Charset.defaultCharset());
		List<String> file2Content = FileUtils.readLines(file2, Charset.defaultCharset());
			
		return file2Content.containsAll(file1Content);
	}


	public boolean contentEquals(File file1, File file2) throws IOException {		
		return FileUtils.contentEquals(file1, file2);
	}

}
