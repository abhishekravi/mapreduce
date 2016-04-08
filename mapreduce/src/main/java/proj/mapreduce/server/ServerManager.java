package proj.mapreduce.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	static private ThreadGroup 		m_clientobsthgrp;
	static private JobScheduler m_jscheduler;
	static private DatasetScheduler m_dsheduler;
	static private List <ClientObserver> m_clientobservers;
	
	public ServerManager(int nclient, String jobname, String input, String output) throws IOException
	{
		m_clientobservers = new ArrayList<ClientObserver>();
		m_serverconf = new ServerConfiguration(nclient);
		
		buildScheduler(jobname);
		
		m_clientobsthgrp = new ThreadGroup("Client Observers");
		
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
			ClientObserver clientobs = new ClientObserver (m_clientobsthgrp, address, port);
			
			clientobs.start();
			m_clientobservers.add(clientobs);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void stopObserver(String address) 
	{

	}
	
	public static void buildScheduler (String jobs)
	{
		m_jscheduler = new JobScheduler();
		m_jscheduler.buid (jobs, m_serverconf.clientCount());
	}
	
	public static void buidDatasetScheduler ()
	{
		m_dsheduler = new DatasetScheduler();
		
	}
}
