package proj.mapreduce.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.net.io.SocketOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.client.MasterObserver;
import proj.mapreduce.main.Server;
import proj.mapreduce.utils.FileOp;
import proj.mapreduce.utils.Utils;
import proj.mapreduce.utils.awshelper.S3Helper;
import proj.mapreduce.utils.network.Command;

/**
 * Gets the job and run it
 * @author all member
 *
 */
public class JobRunner {

	String m_inputdir; 
	String m_args;
	private static Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);
	
	ClientConfiguration m_clientconf;
	
	/**
	 * make job arguments
	 * @param jobName
	 * @param inputToJob
	 * @param outputOfJob
	 */
	public void setArgs(String jobName, String inputToJob, String outputOfJob)
	{
		m_args = jobName + "," + inputToJob + "," + outputOfJob;
	}

	/**
	 * prepare input directory for the job.
	 * @param mode: AWS, HDFS, LOCAL
	 * @param inputToJob: 
	 * @param bucketname
	 * @param listOfFiles
	 * @param awsid
	 * @param awskey
	 */
	public void prepareInput(String mode, String inputToJob, String bucketname, String listOfFiles, String awsid, String awskey) {
		String[] fileList = Utils.parseCSV(listOfFiles);

		switch (mode) {
		case "local":
			String key;
			String inputfolder;
			String path2inputs = listOfFiles;
			inputfolder = m_args.split(",")[0];
			String [] inputArgs = listOfFiles.split(",");
			
			if (FileOp.createFolder(inputfolder)) {
				m_inputdir = inputfolder;
			}

			for (int i = 2; i < inputArgs.length; i++) {
				key = inputArgs[i];
				FileOp.readFromLocal(path2inputs, inputfolder, key);
			}

			break;
		case "aws":
			S3Helper reader = new S3Helper(awsid, awskey);
			FileOp.createFolder(inputToJob);

			for(String fileName : fileList){

				try {
					reader.readFromS3(bucketname, fileName, inputToJob);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			break;
		default:
			break;
		}
	}

	/**
	 * create a process form job and wait for it to be ready
	 * @param jobname
	 * @throws InterruptedException
	 */
	public void runJob (String jobname) throws InterruptedException
	{
		try {
			
			String pbparam = "java,-jar," + m_args;
			
			ProcessBuilder procbuilder = new ProcessBuilder(pbparam.split(","));
			Process proc = procbuilder.start();
			
			BufferedReader errorStream = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			BufferedReader inputStream = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line;
			while((line = errorStream.readLine()) != null){
				LOGGER.error(line);
			}
			while((line = inputStream.readLine()) != null){
				LOGGER.info(line);
			}
			proc.waitFor();
			
			
			MasterObserver.updateServer(makeReply2Server (m_args.split(",")[1]));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param out
	 * @return
	 */
	private String makeReply2Server (String out)
	{
		File outdir = new File (out);
		File [] files = outdir.listFiles();
		
		String command = Command.YARN_COMPLETE_JOB.toString();

		command = command + m_clientconf.getIpaddress() + ":";
		command = command + m_clientconf.ftpPort() + ":";
		command = command + m_clientconf.ftpUser() + ":";
		command = command + m_clientconf.ftpPass() + ":";
		
		
		for (int i = 0; i < files.length; i++)
		{
			if (files[i].isFile())
			{
				command = command + files[i].getName().replace(m_clientconf.ftpPath() + "/", "") + ",";
			}
		}
		
		command = command.substring(0, command.lastIndexOf(","));

		return command;
	}
}
