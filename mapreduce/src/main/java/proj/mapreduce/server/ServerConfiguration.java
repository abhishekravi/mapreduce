package proj.mapreduce.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

public class ServerConfiguration {
	
	/*master configuration such as */
	public InetAddress ip_address;
	
	int m_nclient;
	
	int m_discovery_timeout;
	int	m_ping_timeout;
	int m_ping_frequency;
	HashMap<InetAddress, Boolean> m_neighbors;


	public ServerConfiguration(int nclients, String address) {
		
		m_nclient = nclients;
		m_neighbors = new HashMap<InetAddress, Boolean>();
		try {
			ip_address = InetAddress.getByName(address);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public int clientCount ()
	{
		return m_nclient;
	}
	
	public int discoveyTimeout ()
	{
		return m_discovery_timeout;
	}
	
	public int pingTimeout ()
	{
		return m_ping_timeout;
	}
	
	public int pingFrequency ()
	{
		return m_ping_frequency;
	}
	
	public boolean updateClient (String address)
	{
		try {
			if (!m_neighbors.containsKey(InetAddress.getByName(address)))
			{
				m_neighbors.put(InetAddress.getByName(address), true);
				return true;
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public HashMap<InetAddress, Boolean> 	neighbors()
	{
		return m_neighbors;
	}
}
