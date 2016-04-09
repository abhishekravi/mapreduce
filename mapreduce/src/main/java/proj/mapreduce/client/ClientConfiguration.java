package proj.mapreduce.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.fasterxml.jackson.databind.ser.std.InetAddressSerializer;

import proj.mapreduce.server.ClientObserver;

public class ClientConfiguration {

	boolean m_active = false;
	InetAddress m_address;
	final static int serverport = 9182;
	final static int ftpport = 8291;
	ClientObserver 	 m_observer = null;
	
	public boolean busy()
	{
		return m_active;
	}
	
	public void updateStatus (boolean status)
	{
		m_active = status;
	}
	
	public void assignData(List<String> ds)
	{
		
	}
	
	public static int observerPort ()
	{
		return serverport;
	}
	
	public InetAddress getIpaddress ()
	{
		return m_address;
	}
	
	public void setIpaddressbyName (String ipaddr)
	{
		try {
			m_address = InetAddress.getByName(ipaddr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	public void setObserver(ClientObserver observer)
	{
		m_observer = observer;
	}
	
	public ClientObserver observer()
	{
		return m_observer;
	}
	
}
