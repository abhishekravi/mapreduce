package proj.mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.*;

public class FileOp {

	public static boolean createFolder (String folder)
	{
		File dir = new File (folder);
		return dir.mkdirs();
	}
	
	public static void readFromLocal (String inputfo, String outputfo, String fname)
	{
		Path ip = Paths.get(inputfo + "/" + fname);
		Path op = Paths.get(outputfo + "/" + fname);
		
		try {
			Files.copy(ip, op, REPLACE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
