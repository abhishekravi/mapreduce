package proj.mapreduce.server;

import java.io.IOException;
import java.net.ServerSocket;

import proj.mapreduce.client.ClientManager;
import proj.mapreduce.client.PingTask;
import proj.mapreduce.utils.network.NetworkDiscovery;

public class Listener {

	/* Parse Input Command and take actoin */
	public static void takeAction(String command)
	{
		/* Check if command is from yarn */
		try {
			switch (command.split(":")[0])
			{
			case "yarn":
				ParseYarnCommand(command.substring(command.indexOf(":")));
				break;

			default:
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void ParseYarnCommand (String yarncommand) throws IOException
	{
		String command = yarncommand.split(":")[1];
		String arg = yarncommand.split(":")[2];

		switch (command)
		{
		case "detect":

			if (arg.equals("yes"))
			{
				String address = yarncommand.split(":")[4];
				int obsrvport = Integer.parseInt(yarncommand.split(":")[3]);
		
				if (NetworkDiscovery.updateneighbors(address))
				{
					// ServerManager.startFailureDetection();
					ServerManager.startObserver (address, obsrvport);	
				}				
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
			String name = yarncommand.split(":")[2];
			String input = yarncommand.split(":")[3];
			String output = yarncommand.split(":")[4];
			ClientManager.runJob(name, input, output);
		}
	}


}
