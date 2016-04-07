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
		String command = yarncommand.split(":")[1];
		String arg = yarncommand.split(":")[2];
		
		switch (command)
		{
		case "detect":
			
			if (arg.equals("x"))
			{
				String address = yarncommand.split(":")[3];
				String port = yarncommand.split(":")[4];
				String reply = address + "," + port + "," + "yarn:" + command + ":yes"; 
				PingTask.sendPingReply(reply);
				ClientManager.startObserver ();
			}
			
			break;
		case "ftpserver":
			ClientManager.startFtpServer ();
			break;
		case "ftpclient":
			
			String ftpaddress = yarncommand.split(":")[3];
			int ftpport = Integer.parseInt(yarncommand.split(":")[4]);
			String ftpuser = yarncommand.split(":")[5];
			String ftppass = yarncommand.split(":")[6];
			String serverfile = yarncommand.split(":")[7];
			String localfile = System.getProperty("user.dir") + serverfile;
			
			ClientManager.getfromClient(ftpaddress, ftpport, ftpuser, ftppass, serverfile, localfile);
			break;
		case "hdfs":
			
			String type = yarncommand.split(":")[3];
			String accesskey = yarncommand.split(":")[4];
			String secretkey = yarncommand.split(":")[5];
			String bucketname = yarncommand.split(":")[6];
			String key = yarncommand.split(":")[7];
			
			ClientManager.getfromHdfs(type, accesskey, secretkey, bucketname, key);
			
			break;
		case "runjob":
			ClientManager.runJob();
		}
	}

}
