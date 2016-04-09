package proj.mapreduce.client;

import java.io.IOException;
import java.net.SocketException;

import proj.mapreduce.job.JobRunner;
import proj.mapreduce.utils.awshelper.S3Helper;
import proj.mapreduce.utils.network.NetworkUtils;
import proj.mapreduce.utils.network.ftp.FTPServer;
import proj.mapreduce.utils.network.ftp.FtpClient;

public class ClientManager {

	static boolean m_busy = false;
	static MasterObserver m_observer = null;
	static PingTask m_pingtask = null;
	static FTPServer m_ftpserver = null;
	static FtpClient m_ftpclient = null;
	static String awsid;
	static String awskey;
	static ClientConfiguration m_clientconf;

	public ClientManager(String awsid, String awskey){
		ClientManager.awsid = awsid;
		ClientManager.awskey = awskey;
		
		m_clientconf = new ClientConfiguration();
		m_clientconf.setIpaddressbyName(NetworkUtils.getIpAddress().getHostName());
	}
	
	public ClientManager(String awsid, String awskey, String ftpuser, String ftppass, String ftppath)
	{
		ClientManager.awsid = awsid;
		ClientManager.awskey = awskey;
		
		m_clientconf = new ClientConfiguration();
		m_clientconf.setupFtpConfiguration(ftpuser, ftppass, ftppath);
		
	}

	/* Run a thread to listen for received command in client/server tcp mode */
	public static void startObserver() {
		if (m_pingtask == null)
			return;

		try {
			m_observer = new MasterObserver(m_pingtask.serverAddress());
			m_observer.start();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void startPinging() throws SocketException {

		m_pingtask = new PingTask();
		m_pingtask.start();
		m_busy = true;
	}

	public void startFtpServer() {

		m_ftpserver = new FTPServer(m_clientconf);
		m_ftpserver.createFtpServer();
		m_ftpserver.start();

	}

	public static void getfromClient(String server, int port, String user,
			String password, String serverfile, String localfile)
			throws SocketException, IOException {

		m_ftpclient = new FtpClient(server, port, user, password);
		m_ftpclient.downloadFileBlocking(serverfile, localfile);
		m_ftpclient.stop();
	}

	public static void getfromHdfs(String type, String bucketname, String key) throws IOException {

		switch (type) {
		case "aws":

			S3Helper reader = new S3Helper(ClientManager.awsid, ClientManager.awskey);
			reader.readFromS3(bucketname, key ,"");

			break;
		default:
			break;
		}

	}

	public static void runJob(String jobname, String mode, String bucketname, String listOfFiles, String inputToJob,
			String outputOfJob) {

		JobRunner runner = new JobRunner();
		runner.setArgs(jobname, inputToJob, outputOfJob);
		if (mode.equals("aws"))
			runner.prepareInput(mode, bucketname, listOfFiles, ClientManager.awsid, ClientManager.awskey);
		else
			runner.prepareInput(mode,"", "", "", "");

		try {
			runner.runJob(jobname);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public boolean busy() {
		return m_busy;
	}
}
