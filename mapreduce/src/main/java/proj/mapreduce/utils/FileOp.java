package proj.mapreduce.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

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
	
	public static HashMap<String, Long> getFileBySize(String input)
	{
		File file = new File (input);
		HashMap<String, Long> filelist = new HashMap<String, Long>();
		File [] files;
		
		if (file.isDirectory())
		{
			files = file.listFiles();
			
			for (int i = 0; i < files.length; i++)
			{
				filelist.put(files[i].getName(), files[i].length());
			}
		}
		
		return filelist;
	}
	
}
