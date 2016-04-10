package proj.mapreduce.client;

import java.io.IOException;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.job.JobRunner;
import proj.mapreduce.utils.awshelper.S3Helper;
import proj.mapreduce.utils.network.NetworkUtils;
import proj.mapreduce.utils.network.ftp.FTPServer;
import proj.mapreduce.utils.network.ftp.FtpClient;

/**
 * Client manager class.
 * 
 * @author root
 *
 */
public class ClientManager {
	private static Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);
	static boolean busy = false;
	static MasterObserver observer = null;
	static PingTask pingtask = null;
	static FTPServer ftpserver = null;
	static FtpClient ftpclient = null;
	static String awsid;
	static String awskey;
	static ClientConfiguration clientconf;

	/**
	 * ClientManager constructor.
	 * @param awsid
	 * @param awskey
	 */
	public ClientManager(String awsid, String awskey) {
		ClientManager.awsid = awsid;
		ClientManager.awskey = awskey;

		clientconf = new ClientConfiguration();
		clientconf.setIpaddressbyName(NetworkUtils.getIpAddress().getHostName());
		clientconf.setupFtpConfiguration("geust", "", "/home/ftp");
	}

	/**
	 * ClientManager constructor.
	 * @param awsid
	 * @param awskey
	 * @param ftpuser
	 * @param ftppass
	 * @param ftppath
	 */
	public ClientManager(String awsid, String awskey, String ftpuser, String ftppass, String ftppath) {
		ClientManager.awsid = awsid;
		ClientManager.awskey = awskey;

		clientconf = new ClientConfiguration();
		clientconf.setupFtpConfiguration(ftpuser, ftppass, ftppath);

	}

	/**
	 * Run a thread to listen for received command in client/server tcp mode.
	 */
	public static void startObserver() {
		if (pingtask == null)
			return;

		try {
			observer = new MasterObserver(pingtask.serverAddress());
			observer.start();

		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * start pinging.
	 * @throws SocketException
	 */
	public void startPinging() throws SocketException {

		pingtask = new PingTask();
		pingtask.start();
		busy = true;
	}

	/**
	 * start the FTP server.
	 */
	public void startFtpServer() {

		ftpserver = new FTPServer(clientconf);
		ftpserver.createFtpServer();
		ftpserver.start();

	}

	/**
	 * get files from client.
	 * @param server
	 * @param port
	 * @param user
	 * @param password
	 * @param serverfile
	 * @param localfile
	 * @throws SocketException
	 * @throws IOException
	 */
	public static void getfromClient(String server, int port, String user, String password, String serverfile,
			String localfile) throws SocketException, IOException {

		ftpclient = new FtpClient(server, port, user, password);
		ftpclient.downloadFileBlocking(serverfile, localfile);
		ftpclient.stop();
	}

	/**
	 * method to get files from hdfs.
	 * @param type
	 * @param bucketname
	 * @param key
	 * @throws IOException
	 */
	public static void getfromHdfs(String type, String bucketname, String key) throws IOException {

		switch (type) {
		case "aws":

			S3Helper reader = new S3Helper(ClientManager.awsid, ClientManager.awskey);
			reader.readFromS3(bucketname, key, "");

			break;
		default:
			break;
		}

	}

	/**
	 * run the job using job runner.
	 * @param jobname
	 * @param mode
	 * @param bucketname
	 * @param listOfFiles
	 * @param inputToJob
	 * @param outputOfJob
	 */
	public static void runJob(String jobname, String mode, String bucketname, String listOfFiles, String inputToJob,
			String outputOfJob) {

		JobRunner runner = new JobRunner(clientconf);
		runner.setArgs(jobname, inputToJob, outputOfJob);
		if (mode.equals("aws"))
			runner.prepareInput(mode, inputToJob, bucketname, listOfFiles, ClientManager.awsid, ClientManager.awskey);
		else
			runner.prepareInput(mode, "", "", "", "", "");

		try {
			runner.runJob(jobname);
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage());
		}

	}

	/**
	 * 
	 * @return
	 */
	public boolean busy() {
		return busy;
	}
}
