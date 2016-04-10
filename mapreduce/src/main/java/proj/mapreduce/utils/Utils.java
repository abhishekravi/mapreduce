package proj.mapreduce.utils;

import java.io.IOException;

import com.opencsv.CSVParser;

/**
 * Util class.
 * @author Abhishek Ravi Chandran
 *
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
}
