package proj.mapreduce.job;

import java.io.File;
import java.io.IOException;

import proj.mapreduce.client.MasterObserver;
import proj.mapreduce.utils.FileOp;
import proj.mapreduce.utils.Utils;
import proj.mapreduce.utils.awshelper.S3Helper;
import proj.mapreduce.utils.network.Command;

public class JobRunner {

	String m_inputdir; 
	String m_args;

	public void setArgs(String args)
	{
		m_args = args;
	}

	public void prepareInput(String input, String awsid, String awskey)
	{
		String key;
		String inputfolder;
		String[] inputArgs = Utils.parseCSV(input);
		String fileSystem = inputArgs[0];
		
		

		switch (fileSystem) {
		case "local":
			String path2inputs = inputArgs[1];
			inputfolder = m_args.split(",")[0];

			if (FileOp.createFolder(inputfolder))
			{
				m_inputdir = inputfolder;
			}

			for (int i = 2; i < inputArgs.length; i++)
			{
				key = inputArgs[i];
				FileOp.readFromLocal (path2inputs, inputfolder, key);
			}

			break;
		case "aws":
			inputfolder = inputArgs[1];
			String bucketname = inputArgs[2];
			S3Helper reader = new S3Helper(awsid, awskey); 

			FileOp.createFolder(inputfolder);

			for (int i = 4; i < inputArgs.length; i++)
			{

				key = inputArgs[i];

				try {
					reader.readFromS3(bucketname, key, "");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			break;
		default:
			break;
		}
	}

	public void runJob (String jobname) throws InterruptedException
	{
		try {
			
			String pbparam = "java,-jar," + m_args;
			
			
			ProcessBuilder procbuilder = new ProcessBuilder(pbparam.split(","));
			Process proc = procbuilder.start();
			proc.waitFor();
			
			MasterObserver.updateServer(makeReply (m_args.split(",")[1]));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
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
