package proj.mapreduce.client;

import java.io.IOException;


public class CommandListener{

	public static void takeAction(String command) throws IOException {
		
		/* Check if command is from yarn */
		switch (command.split(":")[0])
		{
		case "yarn":
			ParseYarnCommand(command.substring(command.indexOf(":")));
			break;
			
		default:
			break;
		}
		
	}
	
	private static void ParseYarnCommand (String yarncommand) throws IOException
	{
		String commandArgs[] = yarncommand.split(":");
		String command = commandArgs[1];
		
		switch (command)
		{
		case "detect":
			String arg = commandArgs[2];
			if (arg.equals("x"))
			{
				String address = commandArgs[3];
				String port = commandArgs[4];
				String reply = address + "," + port + "," + "yarn:" + command + ":yes"; 
				PingTask.sendPingReply(reply);
				ClientManager.startObserver ();
			}
			
			break;
		case "ftpclient":
			
			String ftpaddress = commandArgs[3];
			int ftpport = Integer.parseInt(commandArgs[4]);
			String ftpuser = commandArgs[5];
			String ftppass = commandArgs[6];
			String serverfile = commandArgs[7];
			String localfile = System.getProperty("user.dir") + serverfile;
			
			ClientManager.getfromClient(ftpaddress, ftpport, ftpuser, ftppass, serverfile, localfile);
			break;
		case "hdfs":
			
			String type = commandArgs[3];
			String bucketname = commandArgs[6];
			String key = commandArgs[7];
			
			ClientManager.getfromHdfs(type, bucketname, key);
			
			break;
		case "runjob":
			String jobname = commandArgs[2];
			String mode = commandArgs[4];
			String bucketName = commandArgs[5];
			String listOfFiles = commandArgs[6];
			String inputToJob = commandArgs[7];
			String outputOfJob = commandArgs[8];
			
			ClientManager.runJob(jobname, mode, bucketname, listOfFiles, inputToJob, outputOfJob);
		}
	}

}
