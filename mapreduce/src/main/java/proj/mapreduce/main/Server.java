package proj.mapreduce.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.server.ServerManager;

/**
 * Server program main class
 * @author root
 *
 */
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

		if (args.length < 5) {
			return;
		}
		Properties prop = new Properties();
		InputStream input;
		String filename = "config.properties";
		input = Client.class.getClassLoader().getResourceAsStream(filename);
		try {
			prop.load(input);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		LOGGER.info("args:" + args.toString());
		int nclient = Integer.parseInt(args[0]);
		String jobfile = args[1]; /*job.jar,input,output*/
		String mode = args[2]; /* aws, locl or hdfs*/
		String bucket = args[3]; /*bucketname*/
		String folder = args[4]; 

		try {
			m_server = new ServerManager(nclient, jobfile, mode, bucket, folder
					,prop.getProperty("awsid"), prop.getProperty("awskey"));
			m_server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (m_server.busy())
			;

	}

}
