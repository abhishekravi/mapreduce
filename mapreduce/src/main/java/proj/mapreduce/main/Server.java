package proj.mapreduce.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.server.ServerManager;

public class Server {
	private static Logger LOGGER = LoggerFactory.getLogger(Server.class);
	/**
	 * main method to start the server.
	 * 
	 * @param args
	 *            number of clients, jobname, input, output
	 */
	public static void main(String[] args) {

		ServerManager m_server = null;

		if (args.length < 4) {
			return;
		}
		LOGGER.info("args:" + args.toString());
		int nclient = Integer.parseInt(args[0]);
		String jobfile = args[1];
		String input = args[2];
		String output = args[3];
		//String address = args[4];

		try {
			m_server = new ServerManager(nclient, jobfile, input, output, "");
			m_server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (m_server.busy())
			;

	}

}
