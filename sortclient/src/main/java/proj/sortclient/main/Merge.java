package proj.sortclient.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import proj.sortclient.exception.BadDataException;
import proj.sortclient.model.Model;
import proj.sortclient.utils.Utils;

/**
 * Class to merge different files in sorted order.
 * 
 * @author Chintan Pathak, Abhishek Ravi Chandran
 *
 */
public class Merge {

	/**
	 * method to merge all the sorted output files to form a single sorted file.
	 * 
	 * @param inputdir
	 *            dir path of sorted files
	 * @param output
	 *            output path for merged file
	 */
	public static void mergeFiles(String inputdir, String output) {

		File inputDir = new File(inputdir);
		BufferedWriter writer = null;
		try {
			Path path = Paths.get(output + "/op");
			Files.deleteIfExists(path);
			if (!Files.exists(path.getParent()))
				Files.createDirectories(path.getParent());
			Files.createFile(path);

			writer = new BufferedWriter(Files.newBufferedWriter(path, Charset.defaultCharset()));
		} catch (IOException e1) {
			System.err.println("IOException in Merge with message - " + e1.getLocalizedMessage());
		}

		Map<Integer, BufferedReader> readers = new HashMap<Integer, BufferedReader>();
		int counter = 0;

		for (File file : inputDir.listFiles()) {
			if (file.exists() && file.getName().endsWith(".gz")) {
				try {
					readers.put(counter++, new BufferedReader(new FileReader(file)));
				} catch (FileNotFoundException e) {
					// Never going to happen
				}
			}
		}
		String line = "";
		Map<Integer, Model> lines = new HashMap<Integer, Model>();
		Model m;
		for (Integer key : readers.keySet()) {
			try {
				m = new Model();
				line = readers.get(key).readLine();
				m.fill(Utils.parseCSV(line), line);
				lines.put(key, m);
			} catch (NumberFormatException | BadDataException | IOException e) {
				System.err.println("Exception in Merge with message - " + e.getLocalizedMessage());
			}
		}

		Model chosenLine;

		while (!readers.isEmpty()) {
			chosenLine = getSmallest(lines, readers);
			try {
				writer.write(chosenLine.record);
				writer.newLine();
			} catch (IOException e) {
				System.err.println("IOException in Merge with message - " + e.getLocalizedMessage());
			}
		}

		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			System.err.println("IOException in Merge with message - " + e.getLocalizedMessage());
		}
	}

	/**
	 * method to get the line with the smallest value.
	 * 
	 * @param lines
	 *            map of lines
	 * @param readers
	 *            map of readers
	 * @return line to write
	 */
	private static Model getSmallest(Map<Integer, Model> lines, Map<Integer, BufferedReader> readers) {
		Model smallestValue = null;
		float smallVal = Float.MAX_VALUE;
		int smallestIndex = Integer.MAX_VALUE;
		String nextLine;
		Model m;

		for (Integer index : lines.keySet()) {
			if (smallVal >= lines.get(index).dryBulbTemp) {
				smallestValue = lines.get(index);
				smallVal = smallestValue.dryBulbTemp;
				smallestIndex = index;
			}
		}

		try {
			nextLine = readers.get(smallestIndex).readLine();
			if (nextLine != null) {
				m = new Model();
				m.fill(Utils.parseCSV(nextLine), nextLine);
				lines.put(smallestIndex, m);
			} else {
				lines.remove(smallestIndex);
				readers.remove(smallestIndex);
			}
		} catch (NumberFormatException | BadDataException | IOException e) {
			System.err.println("NumberFormatException in getSmallest with message - " + e.getLocalizedMessage());
		}
		return smallestValue;
	}
}
