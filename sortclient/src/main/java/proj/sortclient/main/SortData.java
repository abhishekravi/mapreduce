package proj.sortclient.main;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.sortclient.utils.Constants;
import proj.sortclient.utils.Utils;
/**
 * Main class to sort given file's data
 * @author Abhishek Ravi Chandran
 *
 */
public class SortData {

	private static Logger LOGGER = LoggerFactory.getLogger(SortData.class);

	/**
	 * main method.
	 * @param args
	 * input dir and output dir
	 */
	public static void main(String[] args) {
		if (args.length != Constants.MRARGSIZE) {
			LOGGER.info("usage: <input dir> <output dir>");
			System.exit(0);
		}
		String inputdir = args[0];
		String outputdir = args[1];
		readFiles(inputdir,"tempdir");
		Merge.mergeFiles("tempdir", outputdir);
	}

	/**
	 * method to read the files.
	 * @param inputdir
	 * input path
	 * @param outputdir
	 * output path
	 */
	public static void readFiles(String inputdir, String outputdir) {
		FileSort fileSort = new FileSort();
		List<String> files = Utils.processDir(inputdir);
		for (String file : files) {
			try {
				if(file.endsWith(".gz"))
					fileSort.sortFile(file,outputdir);
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		}
	}

	
}
