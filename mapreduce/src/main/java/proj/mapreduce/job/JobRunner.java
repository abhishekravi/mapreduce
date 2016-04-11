package proj.mapreduce.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.client.MasterObserver;
import proj.mapreduce.utils.FileOp;
import proj.mapreduce.utils.Utils;
import proj.mapreduce.utils.awshelper.S3Helper;
import proj.mapreduce.utils.network.Command;

/**
 * job Runner class.
 * @author raiden
 *
 */
public class JobRunner {

	String inputdir;
	String args;
	private static Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

	ClientConfiguration clientConf;

	public JobRunner(ClientConfiguration clientConf) {
		this.clientConf = clientConf;
	}

	/**
	 * set arguments.
	 * 
	 * @param jobName
	 *            job to run
	 * @param inputToJob
	 *            input folder for job
	 * @param outputOfJob
	 *            output folder of job
	 */
	public void setArgs(String jobName, String inputToJob, String outputOfJob) {
		args = jobName + "," + inputToJob + "," + outputOfJob;
	}

	/**
	 * method to prepare input for job.
	 * 
	 * @param mode
	 * @param inputToJob
	 * @param bucketname
	 * @param listOfFiles
	 * @param awsid
	 * @param awskey
	 */
	public void prepareInput(String mode, String inputToJob, String bucketname, String listOfFiles, String awsid,
			String awskey) {
		// String key;
		// String inputfolder;
		String[] fileList = Utils.parseCSV(listOfFiles);
		switch (mode) {
		case "local":
			// String path2inputs = inputArgs[1];
			// inputfolder = m_args.split(",")[0];
			//
			// if (FileOp.createFolder(inputfolder)) {
			// m_inputdir = inputfolder;
			// }
			//
			// for (int i = 2; i < inputArgs.length; i++) {
			// key = inputArgs[i];
			// FileOp.readFromLocal(path2inputs, inputfolder, key);
			// }
			break;
		case "aws":
			S3Helper reader = new S3Helper(awsid, awskey);
			FileOp.createFolder(inputToJob);
			for (String fileName : fileList) {

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
	 * method to run job.
	 * 
	 * @param jobname
	 *            job name
	 * @throws InterruptedException
	 */
	public void runJob(String jobname) throws InterruptedException {
		try {
			String pbparam = "java,-jar," + args;
			ProcessBuilder procbuilder = new ProcessBuilder(pbparam.split(","));
			Process proc = procbuilder.start();
			BufferedReader errorStream = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			BufferedReader inputStream = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line;
			while ((line = errorStream.readLine()) != null) {
				LOGGER.error(line);
			}
			while ((line = inputStream.readLine()) != null) {
				LOGGER.info(line);
			}
			proc.waitFor();
			MasterObserver.updateServer(makeReply2Server(args.split(",")[2]));
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * method to reply to server.
	 * 
	 * @param out
	 * @return
	 */
	private String makeReply2Server(String out) {
		File outdir = new File(out);
		File[] files = outdir.listFiles();
		String command = Command.YARN_COMPLETE_JOB.toString() + ":";
		command = command + clientConf.getAddress().toString().replace("/", "") + ":";
		command = command + ClientConfiguration.ftpport + ":";
		command = command + clientConf.getFtpuser() + ":";
		command = command + clientConf.getFtppass() + ":";
		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				//command = command + files[i].getParent() + "/" + files[i].getName() + ",";
				command = command + files[i].getAbsolutePath().replace(clientConf.getFtppath() + "/", "") + ",";
			}
		}
		command = command.substring(0, command.lastIndexOf(",")) + "\n";
		return command;
	}
}