package proj.mapreduce.utils.network;

import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Manager {

	int	m_ping_timeout;
	int m_ping_frequency;
	
	TimerTask m_ping_timer_task;
	Timer m_ping_timer;
	
	HashMap<InetAddress, Boolean> 	m_neighbors; 
	
	Manager()
	{	
		m_ping_timer = new Timer(true);
	}
	
	public void Configure()
	{
		/* Discover Network */
		m_neighbors = NetworkDiscovery.discover();
		
		
		/* configuring options*/
		m_ping_timeout = 1000;
		m_ping_frequency = 5000;
	}
	
	
	public void startManager() throws SocketException
	{
		startFailureDetection();
		
		startServer();
	}
	
	public void stopManager ()
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
	
	public void startClients ()
	{
		//connectToClients();
	}
	
	public void stopClients ()
	{
		
	}
	
	public void startServer()
	{
		
	}
	
	public void stopServer()
	{
		
	}
}
