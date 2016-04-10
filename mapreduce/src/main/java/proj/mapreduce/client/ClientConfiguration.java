package proj.mapreduce.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.fasterxml.jackson.databind.ser.std.InetAddressSerializer;

import proj.mapreduce.job.Job;
import proj.mapreduce.server.ClientObserver;

public class ClientConfiguration {

	boolean m_active = false;
	InetAddress m_address;
	final static int serverport = 9182;
	
	final static int m_ftpport = 8291;
	static String m_ftpuser;
	static String m_ftppass;
	static String m_ftppath;
	
	ClientObserver 	 m_observer = null;
	Job				 m_assinedjob = null;

	
	public void setupFtpConfiguration (String user, String password, String path)
	{
		m_ftpuser = user;
		m_ftppass = password;
		m_ftppath = path;
	}
	
	public int ftpPort ()
	{
		return m_ftpport;
	}
	
	public String ftpPass ()
	{
		return m_ftppass;
	}
	
	public String ftpPath()
	{
		return m_ftppath;
	}
	
	public String ftpUser()
	{
		return m_ftpuser;
	}
	
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

	public void setJob (Job job)
	{
		m_assinedjob = job;
	}
	
	public Job job()
	{
		return m_assinedjob;
	}

	
}
