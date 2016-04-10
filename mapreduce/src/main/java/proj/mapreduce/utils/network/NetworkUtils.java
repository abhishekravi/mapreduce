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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network utilities.
 * @author raiden
 *
 */
public class NetworkUtils {

	private static Logger LOGGER = LoggerFactory.getLogger(NetworkUtils.class);
	/**
	 * method to get list of ips.
	 * @return
	 * @throws SocketException
	 */
	public List<String> getIp () throws SocketException {
		
		Enumeration<NetworkInterface> nics_itr = NetworkInterface.getNetworkInterfaces();
		Enumeration<InetAddress> addr_itr;
		List<String>  address = new ArrayList<String>();
		while(nics_itr.hasMoreElements())
		{
			addr_itr = ((NetworkInterface)nics_itr.nextElement()).getInetAddresses();
			while (addr_itr.hasMoreElements())
			{
				address.add(((InetAddress) addr_itr.nextElement()).getHostAddress());
			}
		}
		return address;
	}
	
	public String getIp (String inet) {
		return null;
	}
	
	/**
	 * get ip address.
	 * @param inet
	 * @return
	 * @throws UnknownHostException
	 * @throws SocketException
	 */
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
	
	/**
	 * get subnet.
	 * @param inet
	 * @return
	 * @throws UnknownHostException
	 * @throws SocketException
	 */
	public String getSubnet (int inet) throws UnknownHostException, SocketException {
		
		InetAddress localHost = Inet4Address.getLocalHost();
		NetworkInterface networkInterface = NetworkInterface.getByInetAddress(localHost);
		networkInterface.getInterfaceAddresses().get(inet).getNetworkPrefixLength();
		return null;
	}
	
	/**
	 * method to get broadcast address.
	 * @return
	 */
	public static InetAddress getBroadcastIpAddress ()
	{
		InetAddress address = null;
		try {
			Enumeration<NetworkInterface> interfaces =
				    NetworkInterface.getNetworkInterfaces();
				while (interfaces.hasMoreElements()) {
				  NetworkInterface networkInterface = interfaces.nextElement();
				  if (networkInterface.isLoopback())
				    continue;    // Don't want to broadcast to the loopback interface
				  for (InterfaceAddress interfaceAddress :
				           networkInterface.getInterfaceAddresses()) {
				    address = interfaceAddress.getBroadcast();
				    if (address == null)
				      continue;
				    // Use the address
				  }
				}
		} catch (SocketException e) {
			LOGGER.error(e.getMessage());
		}
		LOGGER.info("broadcast ip:" + address.getHostAddress());
		return address;
	}
	
	/**
	 * method to get ip address
	 * @return
	 */
	public static InetAddress getIpAddress ()
	{
		InetAddress address = null;
		
		try {
			Enumeration<NetworkInterface> interfaces =
				    NetworkInterface.getNetworkInterfaces();
				while (interfaces.hasMoreElements()) {
				  NetworkInterface networkInterface = interfaces.nextElement();
				  if (networkInterface.isLoopback())
				    continue;    // Don't want to broadcast to the loopback interface
				  for (InterfaceAddress interfaceAddress :
				           networkInterface.getInterfaceAddresses()) {
				    address = interfaceAddress.getAddress();
				    if (address == null)
				      continue;
				  }
				}
		} catch (SocketException e) {
			LOGGER.error(e.getMessage());
		}
		return address;
	}
}
