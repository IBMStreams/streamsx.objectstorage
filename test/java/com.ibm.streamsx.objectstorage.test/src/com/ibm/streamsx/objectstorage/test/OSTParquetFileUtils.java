package com.ibm.streamsx.objectstorage.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

public class OSTParquetFileUtils implements OSTFileUtils {

	private static final OSTParquetFileUtils instance = new OSTParquetFileUtils();

	private OSTParquetFileUtils() {
	}

	public static OSTParquetFileUtils getInstance() {
		return instance;
	}

	@SuppressWarnings("deprecation")
	@Override
	public List<String> readFileLineByLine(String inFile) throws IOException {

		List<String> res = new LinkedList<>();
		ParquetReader<SimpleRecord> reader = null;
		try {
			StringWriter strOut = new StringWriter();
			PrintWriter writer = new PrintWriter(strOut);			
			reader = new ParquetReader<SimpleRecord>(new Path(inFile), new SimpleReadSupport());			
			for (SimpleRecord value = (SimpleRecord) reader.read(); value != null; value = (SimpleRecord) reader
					.read()) {
				value.prettyPrint(writer);
				writer.flush();
				res.add(strOut.toString());
			}
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception localException1) {
				}
			}
		}

		return res;
	}

	@Override
	public void showFileDiffs(File file1, File file2) throws Exception {
		List<String> file1Content = readFileLineByLine(file1.getAbsolutePath());
		List<String> file2Content = readFileLineByLine(file2.getAbsolutePath());
		List<String> deltaList = new ArrayList<String>(file1Content);
		deltaList.removeAll(file2Content);

		if (deltaList.size() == 0 ) {
			System.out.println("No data in '" + file1.getPath() + "' that doesn't exist in '" + file2.getPath() + "'");
		} else {
			System.out.println("Exists in '" + file1.getPath() + "', but missing in '" + file2.getPath() + "'");
			for (int i = 0; i < deltaList.size(); i++) {
				System.out.println(deltaList.get(i));
			}
		}
		
		file2Content.removeAll(file1Content);
		if (file2Content.size() == 0) {
			System.out.println("No data in '" + file2.getPath() + "' that doesn't exist in '" + file1.getPath() + "'");
		} else {
			System.out.println("Exists in '" + file2.getPath() + "', but missing in '" + file1.getPath() + "'");
			for (int i = 0; i < file2Content.size(); i++) {
				System.out.println(file2Content.get(i));
			}
		}
	}

	@Override
	public boolean contentContains(File file1, File file2) throws IOException {
		Set<String> file1Content = readFileLineByLine(file1.getAbsolutePath()).stream().collect(Collectors.toSet());
		Set<String> file2Content = readFileLineByLine(file2.getAbsolutePath()).stream().collect(Collectors.toSet());
		
		return file1Content.containsAll(file2Content);
	}

	@Override
	public boolean contentEquals(File file1, File file2) throws IOException {
		List<String> file1Content = readFileLineByLine(file1.getAbsolutePath());
		List<String> file2Content = readFileLineByLine(file2.getAbsolutePath());
		
		
		return file1Content.equals(file2Content);
	}
	
//	public static void main(String[] args) {
//		try {
//			File file1 = new File("/tmp/fileTestCloseByTimeParquetGzip0.parquet");
//			File file2 = new File("/tmp/fileTestCloseByTimeParquetGzip0_expected.parquet");
//			//OSTParquetFileUtils.getInstance().showFileDiffs(file1, file2);
//			
//			boolean expectedEqActual = OSTParquetFileUtils.getInstance().contentContains(file1, file2);
//			if (!expectedEqActual) {
//				System.out.println("-> EXPECTED AND ACTUAL ARE DIFFERENT");				
//			} else {
//				System.out.println("-> ACTUAL CONTAINS ALL EXPECTED CONTENT");
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}
