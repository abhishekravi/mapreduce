package proj.mapreduce.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.job.Dataset;
import proj.mapreduce.job.DatasetScheduler;
import proj.mapreduce.job.Job;
import proj.mapreduce.job.JobScheduler;
import proj.mapreduce.utils.KeyPair;
import proj.mapreduce.utils.network.Command;
import proj.mapreduce.utils.network.NetworkDiscovery;

/**
 * ServerManager class.
 * @author all team
 *
 */
public class ServerManager {

	private static Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);
	boolean active = false;
	static TimerTask pingTimerTask;
	static Timer pingTimer;
	static private ServerConfiguration serverconf;
	static private ThreadGroup clientobsthgrp;
	static private JobScheduler jscheduler;
	static private DatasetScheduler dsheduler;

	/**
	 * 
	 * @param nclient
	 * @param job
	 * @param mode
	 * @param bucketname
	 * @param folder
	 * @param awsid
	 * @param awskey
	 * @throws IOException
	 */
	public ServerManager(int nclient, String job, String mode, String bucketname, String folder, String awsid,
			String awskey) throws IOException {
		serverconf = new ServerConfiguration(nclient);
		String[] jobargs = job.split(",");
		buildScheduler();
		buildJobPool(jobargs[0], jobargs[1], jobargs[2]);
		buidDatasetScheduler(mode, bucketname, folder, awsid, awskey);
		clientobsthgrp = new ThreadGroup("Client Observers");
	}

	/**
	 * 
	 */
	private void buildScheduler() {
		jscheduler = new JobScheduler(serverconf);
	}

	/**
	 * 
	 * @param jobs
	 * @param input
	 * @param output
	 */
	public static void buildJobPool(String jobs, String input, String output) {
		jscheduler.buid(jobs, input, output, serverconf.clientCount());
	}

	/**
	 * 
	 * @param mode
	 * @param bucketname
	 * @param folder
	 * @param awsid
	 * @param awskey
	 */
	public static void buidDatasetScheduler(String mode, String bucketname, String folder, String awsid,
			String awskey) {
		dsheduler = new DatasetScheduler(serverconf);
		Dataset dataset = new Dataset(mode, bucketname, folder, awsid, awskey);
		dsheduler.addDataset(dataset);
	}

	/**
	 * 
	 * @throws SocketException
	 */
	public void start() throws SocketException {
		NetworkDiscovery netdisk = new NetworkDiscovery(serverconf);
		netdisk.discover();
	}

	/**
	 * 
	 */
	public void stop() {
		stopFailureDetection();
	}

	/**
	 * 
	 * @throws SocketException
	 */
	public static void startFailureDetection() throws SocketException {
		if (serverconf.clientCount() == 0) {
			// m_ping_timer_task = new PingTask(m_serverconf.neighbors(),
			// m_serverconf.pingTimeout());
			// m_ping_timer.scheduleAtFixedRate(m_ping_timer_task, 0,
			// m_serverconf.pingFrequency());
		}
	}

	/**
	 * 
	 */
	public void stopFailureDetection() {
		pingTimer.cancel();
	}

	/**
	 * 
	 * @return
	 */
	public boolean busy() {
		return active;
	}

	/**
	 * create a thread which is a tcpclient thread to send data to a specific
	 * tcpserver which is one our clients
	 * 
	 * @param address
	 */
	public static void startObserver(String address, int port) {
		LOGGER.info("address:" + address);
		ClientObserver clientobs = new ClientObserver(clientobsthgrp, address, port);
		if (serverconf.addObserver(address, clientobs)) {
			clientobs.start();
		}
	}

	/**
	 * 
	 * @param address
	 */
	public static void stopObserver(String address) {

	}

	/**
	 * 
	 * @param address
	 * @param obsrvport
	 * @return
	 */
	public static boolean updateNeighbors(String address, int obsrvport) {
		if (!serverconf.updateClient(address)) {
			return false;
		}
		ServerManager.startObserver(address, obsrvport);
		if (serverconf.activeNeighbors() >= serverconf.clientCount()) {
			ServerManager.startScheduling();
			NetworkDiscovery.stop();
		}
		return true;
	}

	/**
	 * 
	 */
	public static void startScheduling() {
		dsheduler.schedule(0);
		for (int i = 0; i < serverconf.clientCount(); i++) {
			KeyPair<Job, ClientConfiguration> pair = jscheduler.scheduleNextJob();
			if (serverconf.observedClient(pair.getRight().getAddress()).toString() != "") {
				/* create job command */
				Job job = pair.getLeft();
				job.addDataset(dsheduler.getType(), dsheduler.getBucketname(), dsheduler.getNextChunk());
				String command = Command.YARN_RUN_JOB.toString();
				command = command + job.toString() + "\n";
				try {
					serverconf.observedClient(pair.getRight().getAddress()).sendCommand(command);
				} catch (IOException e) {
					LOGGER.error(e.getMessage());
				}
			}
		}
		dsheduler.removeDataset(0);
	}

	/**
	 * 
	 * @param ip
	 * @param port
	 * @param user
	 * @param pass
	 * @param intermediatefiles
	 */
	public static void mapperComplete(String ip, int port, String user, String pass, String intermediatefiles) {
		/* make the clientconf to inactive */
		try {
			if (serverconf.getClientConfiguration().keySet().contains(InetAddress.getByName(ip))) {
				serverconf.getClientConfiguration().get(InetAddress.getByName(ip)).setActive(false);
			}
		} catch (UnknownHostException e) {
			LOGGER.error(e.getMessage());
		}
		/* create dataset from type hdfs */
		Dataset dataset = new Dataset(ip, port, user, pass, intermediatefiles);
		dsheduler.addDataset(dataset);
	}

	/**
	 * 
	 */
	public static void shuffle() {
		Collection<ClientConfiguration> clientaddr = serverconf.getClientConfiguration().values();
		ArrayList<List<String>> chunks;
		/* get all clients */
		for (ClientConfiguration conf : clientaddr) {
			if (conf.isActive())
				return;
		}
		/* schedule dataset */
		chunks = dsheduler.reducerSchedule();
		createReducerCommand(chunks);
	}

	/**
	 * 
	 * @param allchunks
	 */
	private static void createReducerCommand(ArrayList<List<String>> allchunks) {
		/* loop over chunks */
		String chunkstring = "";
		String command = "";
		for (List<String> liststr : allchunks) {
			chunkstring += liststr.remove(liststr.size() - 1);
			for (String str : liststr) {
				chunkstring += str;
				chunkstring += ",";
			}
			/* remove * from last */
			chunkstring.substring(0, chunkstring.length() - 1);
			chunkstring += ":";
		}
		chunkstring.substring(0, chunkstring.length() - 1);
		command = Command.YARN_DO_REDUCER.toString() + ":" + chunkstring + "\n";
		InetAddress ips = serverconf.getClientConfiguration().keySet().iterator().next();
		try {
			serverconf.observedClient(ips).sendCommand(command);
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}
}
