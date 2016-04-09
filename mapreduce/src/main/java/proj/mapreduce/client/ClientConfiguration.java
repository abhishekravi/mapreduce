package proj.mapreduce.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.fasterxml.jackson.databind.ser.std.InetAddressSerializer;

public class ClientConfiguration {

	boolean m_active = false;
	static InetAddress m_address;
	final static int serverport = 9182;
	
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
	
	public static void setIpaddressbyName (String ipaddr)
	{
		try {
			m_address = InetAddress.getByName(ipaddr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
}
