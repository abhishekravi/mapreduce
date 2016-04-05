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
		String command = yarncommand.split(":")[0];
		String arg = yarncommand.split(":")[1];
		
		switch (command)
		{
		case "detect":
			
			if (arg.equals("x"))
			{
				String address = yarncommand.split(":")[2];
				String port = yarncommand.split(":")[3];
				String reply = address + "," + port + "," + "yarn:" + command + ":yes"; 
				PingTask.sendPingReply(reply);
				ClientManager.startObserver ();
			}
			
			break;
		case "ftpserver":
			ClientManager.startFtpServer ();
			break;
		case "ftpclient":
			
			String ftpaddress = yarncommand.split(":")[2];
			int ftpport = Integer.parseInt(yarncommand.split(":")[3]);
			String ftpuser = yarncommand.split(":")[4];
			String ftppass = yarncommand.split(":")[5];
			String serverfile = yarncommand.split(":")[6];
			String localfile = System.getProperty("user.dir") + serverfile;
			
			ClientManager.getfromClient(ftpaddress, ftpport, ftpuser, ftppass, serverfile, localfile);
			break;
		case "hdfs":
			
			String type = yarncommand.split(":")[2];
			String accesskey = yarncommand.split(":")[3];
			String secretkey = yarncommand.split(":")[4];
			String bucketname = yarncommand.split(":")[5];
			String key = yarncommand.split(":")[6];
			
			ClientManager.getfromHdfs(type, accesskey, secretkey, bucketname, key);
			
			break;
		case "runjob":
			ClientManager.runJob();
		}
	}

}
