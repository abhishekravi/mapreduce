package proj.mapreduce.server;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.client.ClientConfiguration;

public class ServerConfiguration {
	
	private static Logger LOGGER = LoggerFactory.getLogger(ServerConfiguration.class);
	
	/*master configuration such as */
	public InetAddress ip_address;
	
	int m_nclient;
	
	int m_discovery_timeout;
	int	m_ping_timeout;
	int m_ping_frequency;
	HashMap<InetAddress, Boolean> m_neighbors;
	String awsid;
	String awskey;
	private static Map <InetAddress, ClientConfiguration> m_clientconf;	

	public ServerConfiguration(int nclients, String address) {

		
		m_nclient = nclients;
		m_neighbors = new HashMap<InetAddress, Boolean>();
		m_clientconf = new HashMap <InetAddress, ClientConfiguration>();
		
		//fetching ip address of current machine
		try {
			Enumeration<NetworkInterface> interfaces =
				    NetworkInterface.getNetworkInterfaces();
				while (interfaces.hasMoreElements()) {
				  NetworkInterface networkInterface = interfaces.nextElement();
				  if (networkInterface.isLoopback())
				    continue;    // Don't want to broadcast to the loopback interface
				  for (InterfaceAddress interfaceAddress :
				           networkInterface.getInterfaceAddresses()) {
				    this.ip_address = interfaceAddress.getBroadcast();
				    if (this.ip_address == null)
				      continue;
				    // Use the address
				  }
				}
				LOGGER.info("broadcast address:" + this.ip_address.getHostAddress());
		} catch (SocketException e) {
			LOGGER.error(e.getMessage());
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
			if (!m_clientconf.containsKey(InetAddress.getByName(address)))
			{
				ClientConfiguration clientconf = new ClientConfiguration();
				clientconf.setIpaddressbyName(address);
				clientconf.updateStatus(false);
				
				m_clientconf.put(InetAddress.getByName(address), clientconf);
				
				return true;
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public int	activeNeighbors()
	{
		return m_clientconf.size();
	}
	
	public Map <InetAddress, ClientConfiguration> getClientConfiguration()
	{
		return m_clientconf;
	}
	
	public boolean addObserver(String address, ClientObserver observer)
	{
		try {
			if (m_clientconf.containsKey(InetAddress.getByName(address)))
			{
				m_clientconf.get(InetAddress.getByName(address)).setObserver(observer);
				return true;
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return false;
	}

	public ClientObserver observedClient(InetAddress ipaddress) {
		
		if (m_clientconf.containsKey(ipaddress));
		{
			return m_clientconf.get(ipaddress).observer();
		}
	}
	 
}
