package proj.mapreduce.main;

/** Client program main class
 *  all team member
 */
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import proj.mapreduce.client.ClientManager;
import proj.mapreduce.client.CommandListener;
import proj.mapreduce.server.Listener;
import proj.mapreduce.utils.network.Command;

public class Client {

	/**
	 * main method for client.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Properties prop = new Properties();
		InputStream input;
		String filename = "config.properties";
		input = Client.class.getClassLoader().getResourceAsStream(filename);
		prop.load(input);
		ClientManager m_client = new ClientManager(prop.getProperty("awsid"), prop.getProperty("awskey"), "guest", "",
				"/home/");
		m_client.startPinging();
		m_client.startFtpServer();
		
		/* just to test ftp and calling merge*/
		//CommandListener.takeAction(Command.YARN_DO_REDUCER.toString() + ":172.16.202.1,8291,guest,,createInstances.sh,generate.sh,pom.xml");
		
		
		while (m_client.busy());
	}
}