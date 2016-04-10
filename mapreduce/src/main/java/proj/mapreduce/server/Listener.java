package proj.mapreduce.server;

import java.io.IOException;

/**
 * Listener class to get communication from clients.
 * 
 * @author root
 *
 */
public class Listener {

	/**
	 * Parse Input Command and take action
	 * 
	 * @param command
	 */
	public static void takeAction(String command) {
		/* Check if command is from yarn */
		try {
			switch (command.split(":")[0]) {
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

	/**
	 * parse command
	 * 
	 * @param yarncommand
	 * @throws IOException
	 */
	private static void ParseYarnCommand(String yarncommand) throws IOException {
		String command = yarncommand.split(":")[1];
		String arg = yarncommand.split(":")[2];
		switch (command) {
		case "detect":

			if (arg.equals("yes")) {
				String address = yarncommand.split(":")[4];
				int obsrvport = Integer.parseInt(yarncommand.split(":")[3]);

				ServerManager.updateNeighbors(address, obsrvport);
			}
			break;
		case "jobcomp":
			String[] clientreply = yarncommand.split(":");
			String ftpaddress = clientreply[2];
			int ftpport = Integer.parseInt(clientreply[3]);
			String ftpuser = clientreply[4];
			String ftppass = clientreply[5];
			String intermediatefile = clientreply[6];
			ServerManager.mapperComplete(ftpaddress, ftpport, ftpuser, ftppass, intermediatefile);
			ServerManager.shuffle();
			break;
		default:
			break;
		}
	}
}
