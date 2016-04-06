package proj.sortclient.utils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVParser;

/**
 * 
 * @author Chintan Pathak, Abhishek Ravi Chandran
 * @author Mania Abdi, Chinmayee Vaidya
 *
 */

/*
 * This class provides various utility functions to use in doing the required
 * processing
 */
public class Utils {

	final static CSVParser PARSER = new CSVParser();

	/**
	 * method to parse csv data.
	 * 
	 * @param value
	 *            csv data
	 * @return string array
	 */
	public static String[] parseCSV(String value) {
		String parsed[] = null;
		try {
			parsed = PARSER.parseLine(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return parsed;
	}

	/**
	 * method to read the directory and get the list of all files.
	 * 
	 * @param dirPath
	 *            directory path
	 * @return list of file names with path
	 */
	public static List<String> processDir(String dirPath) {
		// get all filename in the directory
		List<String> files = new ArrayList<String>();
		try (DirectoryStream<Path> directoryStream = Files
				.newDirectoryStream(Paths.get(dirPath))) {
			for (Path path : directoryStream) {
				files.add(path.toString());
			}
		} catch (IOException ex) {
			new Error(ex);
		}
		return files;
	}
	
	/**
	 * method to get file name from full path.
	 * @param path
	 * path
	 * @return
	 * file name
	 */
	public static String getFileName(String path){
		String[] patharray = path.split("/");
		String fileName = patharray[patharray.length - 1];
		return fileName.substring(0, fileName.indexOf("."));
	}

}
