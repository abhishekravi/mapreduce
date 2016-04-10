package proj.mapreduce.experiment.merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Merge {

	public static void main(String[] args) {

		File inputDir = new File(args[1]);
		File outputFile = new File(args[2]);
		BufferedWriter writer = null;

		try {
			writer = new BufferedWriter(new FileWriter(outputFile));
		} catch (IOException e1) {
			System.err.println("IOException in Merge with message - " + e1.getLocalizedMessage());
		}

		Map<Integer, BufferedReader> readers = new HashMap<Integer, BufferedReader>();
		int counter = 0;

		for (File file : inputDir.listFiles()) {
			if (file.exists()) {
				
				if (file.isDirectory())
				{
					for (File subdirfile : file.listFiles())
					{
						try {
							readers.put(counter++, new BufferedReader(new FileReader(subdirfile)));
						} catch (FileNotFoundException e) {
							// Never going to happen
						}
					}
				}
				else
				{
					try {
						readers.put(counter++, new BufferedReader(new FileReader(file)));
					} catch (FileNotFoundException e) {
						// Never going to happen
					}
				}
			}
		}

		Map<Integer, Integer> lines = new HashMap<Integer, Integer>();

		for (Integer key : readers.keySet()) {
			try {
				lines.put(key, Integer.parseInt(readers.get(key).readLine()));
			} catch (NumberFormatException e) {
				System.err.println("NumberFormatException in Merge with message - " + e.getLocalizedMessage());
			} catch (IOException e) {
				System.err.println("IOException in Merge with message - " + e.getLocalizedMessage());
			}
		}

		Integer chosenLine;

		while (!readers.isEmpty()) {
			chosenLine = getSmallest(lines, readers);
			try {
				writer.write(String.valueOf(chosenLine));
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

	private static Integer getSmallest(Map<Integer, Integer> lines, Map<Integer, BufferedReader> readers) {
		Integer smallestValue = Integer.MAX_VALUE;
		Integer smallestIndex = Integer.MAX_VALUE;
		String nextLine;

		for (Integer index : lines.keySet()) {
			if (smallestValue >= lines.get(index)) {
				smallestValue = lines.get(index);
				smallestIndex = index;
			}
		}

		try {
			nextLine = readers.get(smallestIndex).readLine();
			if (nextLine != null) {
				lines.put(smallestIndex, Integer.parseInt(nextLine));
			} else {
				lines.remove(smallestIndex);
				readers.remove(smallestIndex);
			}
		} catch (NumberFormatException e) {
			System.err.println("NumberFormatException in getSmallest with message - " + e.getLocalizedMessage());
		} catch (IOException e) {
			System.err.println("IOException in getSmallest with message - " + e.getLocalizedMessage());
		}
		return smallestValue;
	}
}
