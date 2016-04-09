package proj.mapreduce.utils.network;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import com.sun.org.apache.xerces.internal.xs.StringList;

public class NetworkUtils {

	public StringList getIp () throws SocketException {
		
		Enumeration nics_itr = NetworkInterface.getNetworkInterfaces();
		Enumeration addr_itr;
		List<String>  address = new ArrayList<String>();
		
		while(nics_itr.hasMoreElements())
		{
			addr_itr = ((NetworkInterface)nics_itr.nextElement()).getInetAddresses();
			while (addr_itr.hasMoreElements())
			{
				address.add(((InetAddress) addr_itr.nextElement()).getHostAddress());
				
			}
		}
		
		return null;
	}
	
	public String getIp (String inet) {
		return null;
	}
	
	public String getIp (int inet) throws UnknownHostException, SocketException {
		
		InetAddress localHost = Inet4Address.getLocalHost();
		NetworkInterface networkInterface = NetworkInterface.getByInetAddress(localHost);
		networkInterface.getInterfaceAddresses().get(0).getNetworkPrefixLength();
		
		return null;
	}
	
	public String[] getSubnet () {
		return null;
	}
	
	public String getSubnet (String inet) {
		return null;
	}
	
	public String getSubnet (int inet) throws UnknownHostException, SocketException {
		
		InetAddress localHost = Inet4Address.getLocalHost();
		NetworkInterface networkInterface = NetworkInterface.getByInetAddress(localHost);
		networkInterface.getInterfaceAddresses().get(inet).getNetworkPrefixLength();
		
		return null;
	}
}
