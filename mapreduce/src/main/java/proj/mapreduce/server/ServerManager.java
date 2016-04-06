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

	int	m_ping_timeout;
	int m_ping_frequency;

	
	boolean m_active = false;
	
	TimerTask m_ping_timer_task;
	Timer m_ping_timer;
	
	HashMap<InetAddress, Boolean> 	m_neighbors;
	HashMap<InetAddress, Integer> 	m_neighborsconfig;

	JobScheduler m_jscheduler;
	DatasetScheduler m_dsheduler;

	public ServerManager() throws IOException
	{	
		m_ping_timer = new Timer(true);
		Configure();
	}
	
	private void Configure() throws IOException
	{
		/* Discover Network */
		m_neighbors = NetworkDiscovery.discover();
		
		/* configuring options*/
		m_ping_timeout = 1000;
		m_ping_frequency = 5000;
		
	}
	
	
	public void start () throws SocketException
	{		
		startFailureDetection();
	}
	
	public void stop ()
	{
		stopFailureDetection();
	}
	
	
	public void startFailureDetection() throws SocketException
	{
		 m_ping_timer_task = new PingTask(m_neighbors, m_ping_timeout);
		 m_ping_timer.scheduleAtFixedRate(m_ping_timer_task, 0, m_ping_frequency);
	}
	
	public void stopFailureDetection()
	{
		m_ping_timer.cancel();
	}

}
