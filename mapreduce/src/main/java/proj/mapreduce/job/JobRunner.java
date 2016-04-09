package proj.mapreduce.job;

import java.io.File;
import java.io.IOException;

import proj.mapreduce.client.MasterObserver;
import proj.mapreduce.utils.awshelper.S3Reader;
import proj.mapreduce.utils.network.Command;

public class JobRunner {

	String m_inputdir; 
	
	public void prepareInput(String input)
	{
		String hdfs = input.split(",")[0];

		switch (hdfs) {
		case "aws":
			String key;
			String inputfolder = input.split(",")[1];
			String bucketname = input.split(",")[2];
			String accesskey = input.split(",")[3];
			String privatekey = input.split(",")[4];
			S3Reader reader = new S3Reader(accesskey, privatekey); 
		
			
			/*Create Folder*/
			createFolder(inputfolder);
			
			for (int i = 4; i < input.split(",").length; i++)
			{
				
				key = input.split(",")[i];
				
				try {
					reader.readFromS3(bucketname, key);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			break;
		case "local":
			break;
		default:
			break;
		}
	}
	
	private void createFolder (String folder)
	{
		File dir = new File (folder);
		if (dir.mkdirs())
		{
			m_inputdir = folder;
		}
	}
	
	public void runJob (String jobname, String output) throws IOException
	{
		// run job
		ProcessBuilder procbuilder = new ProcessBuilder("java", "-jar", jobname);
		Process proc = procbuilder.start();
		
		// wait for finish 
		
		/**/
		
		MasterObserver.updateServer(makeReply (output));
		

	}

	private String makeReply (String out)
	{
		File outdir = new File (out);
		File [] files = outdir.listFiles();
		
		String command = Command.YARN_COMPLETE_JOB.toString();
		
		for (int i = 0; i < files.length; i++)
		{
			if (files[i].isFile())
			{
				command = command + ":" + files[i].toString();
			}
			
		}
		
		return command;
	}
}
