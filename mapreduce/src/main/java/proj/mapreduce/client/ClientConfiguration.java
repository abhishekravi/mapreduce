
package proj.mapreduce.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.fasterxml.jackson.databind.ser.std.InetAddressSerializer;

import proj.mapreduce.job.Job;
import proj.mapreduce.server.ClientObserver;

/**
 * 
 * @author all team
 *
 */
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

	/**
	 * 
	 * @param user
	 * @param password
	 * @param path
	 */
	public void setupFtpConfiguration (String user, String password, String path)
	{
		m_ftpuser = user;
		m_ftppass = password;
		m_ftppath = path;
	}
	
	/**
	 * 
	 * @return
	 */
	public int ftpPort ()
	{
		return m_ftpport;
	}
	
	/**
	 * 
	 * @return
	 */
	public String ftpPass ()
	{
		return m_ftppass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String ftpPath()
	{
		return m_ftppath;
	}
	
	/**
	 * 
	 * @return
	 */
	public String ftpUser()
	{
		return m_ftpuser;
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean busy()
	{
		return m_active;
	}
	
	/**
	 * 
	 * @param status
	 */
	public void updateStatus (boolean status)
	{
		m_active = status;
	}
	
	/**
	 * 
	 * @param ds
	 */
	public void assignData(List<String> ds)
	{
		
	}
	
	/**
	 * 
	 * @return
	 */
	public static int observerPort ()
	{
		return serverport;
	}
	
	/**
	 * 
	 * @return
	 */
	public InetAddress getIpaddress ()
	{
		return m_address;
	}
	
	/**
	 * 
	 * @param ipaddr
	 */
	public void setIpaddressbyName (String ipaddr)
	{
		try {
			m_address = InetAddress.getByName(ipaddr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 * @param observer
	 */
	public void setObserver(ClientObserver observer)
	{
		m_observer = observer;
	}
	
	/**
	 * 
	 * @return
	 */
	public ClientObserver observer()
	{
		return m_observer;
	}

	/**
	 * 
	 * @param job
	 */
	public void setJob (Job job)
	{
		m_assinedjob = job;
	}
	
	/**
	 * 
	 * @return
	 */
	public Job job()
	{
		return m_assinedjob;
	}

	
}
