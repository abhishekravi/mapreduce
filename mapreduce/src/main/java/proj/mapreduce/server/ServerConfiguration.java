package proj.mapreduce.server;

import java.net.InetAddress;

public class ServerConfiguration {
	
	/*master configuration such as */
	InetAddress ip_address;
	
	static int m_nclient;
	
	public static int clientCount ()
	{
		return m_nclient;
	}
}
