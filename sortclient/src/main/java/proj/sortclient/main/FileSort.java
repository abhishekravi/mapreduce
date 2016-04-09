package proj.sortclient.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.sortclient.exception.BadDataException;
import proj.sortclient.model.Model;
import proj.sortclient.utils.Utils;
/**
 * class that sorts each files data.
 * @author Abhishek Ravi Chandran
 *
 */
public class FileSort {
	private static Logger LOGGER = LoggerFactory.getLogger(SortData.class);

	/**
	 * method to sort a files data.
	 * @param file
	 * file to sort
	 * @param outputdir
	 * dir to write the file
	 * @throws IOException
	 */
	public void sortFile(String file, String outputdir) throws IOException {
		List<Model> data = new ArrayList<Model>();
		InputStream gzipStream = new GZIPInputStream(new FileInputStream(file));
		BufferedReader buffer = new BufferedReader(new InputStreamReader(gzipStream));
		Model m;
		String line = "";
		while ((line = buffer.readLine()) != null) {
			m = new Model();
			try {
				m.fill(Utils.parseCSV(line), line);
				data.add(m);
			} catch (BadDataException e) {
				continue;
			}
		}
		buffer.close();
		Collections.sort(data, new TempComp());
		writeFile(data, file, outputdir);
	}

	/**
	 * method to write the file to the output dir.
	 * @param data
	 * data to write
	 * @param file
	 * file to write
	 * @param outputdir
	 * output directory
	 */
	private void writeFile(List<Model> data, String file, String outputdir) {
		Path path = Paths.get(outputdir + "/" + Utils.getFileName(file));
		
		try {
			Files.deleteIfExists(path);
			if(!Files.exists(path.getParent()))
					Files.createDirectories(path.getParent());
			Files.createFile(path);
			BufferedWriter bw = new BufferedWriter(Files.newBufferedWriter(path, Charset.defaultCharset()));
			for (Model m : data) {
				bw.append(m.record);
				bw.newLine();
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			System.err.println(e.getMessage());
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * Custom comparator to sort according to temperature in ascending order.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	class TempComp implements Comparator<Model> {

		@Override
		public int compare(Model e1, Model e2) {
			if (e1.dryBulbTemp < e2.dryBulbTemp)
				return -1;
			else if (e1.dryBulbTemp > e2.dryBulbTemp)
				return 1;
			else
				return 0;
		}
	}
}
