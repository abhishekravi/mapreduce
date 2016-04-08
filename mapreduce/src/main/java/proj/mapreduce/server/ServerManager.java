package proj.mapreduce.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import proj.mapreduce.job.DatasetScheduler;
import proj.mapreduce.job.JobScheduler;
import proj.mapreduce.utils.network.NetworkDiscovery;
import proj.mapreduce.utils.network.PingTask;


public class ServerManager {

	boolean m_active = false;

	static TimerTask m_ping_timer_task;
	static Timer m_ping_timer;

	static private ServerConfiguration m_serverconf;
	static private ThreadGroup 			m_clientobsth;
	private JobScheduler m_jscheduler;
	private DatasetScheduler m_dsheduler;
	
	public ServerManager() throws IOException
	{
		Configure();
	}

	private void Configure() throws IOException
	{
		m_serverconf = new ServerConfiguration();
		m_clientobsth = new ThreadGroup("Client Observers");
	}


	public void start () throws SocketException
	{
		NetworkDiscovery netdisk = new NetworkDiscovery(m_serverconf);
		netdisk.discover();
	}

	public void stop ()
	{
		stopFailureDetection();
	}


	public static void startFailureDetection() throws SocketException
	{
		if (m_serverconf.clientCount() == 0)
		{
			m_ping_timer_task = new PingTask(m_serverconf.neighbors(), m_serverconf.pingTimeout());
			m_ping_timer.scheduleAtFixedRate(m_ping_timer_task, 0, m_serverconf.pingFrequency());
		}
	}

	public void stopFailureDetection()
	{
		m_ping_timer.cancel();
	}

	public boolean busy() {
		return m_active;
	}

	/**
	 * create a thread which is a tcpclient thread to send data to a specific tcpserver which is one our clients 
	 * @param address
	 */
	public static void startObserver(String address, int port) {
				
		try {
			ClientObserver clientobs = new ClientObserver (m_clientobsth, address, port);
			clientobs.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public static void stopObserver(String address) {


	}
}
