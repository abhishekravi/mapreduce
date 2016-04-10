package proj.mapreduce.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.utils.network.NetworkUtils;

/**
 * a container for server configuraion
 * 
 * @author root
 *
 */
public class ServerConfiguration {

	private static Logger LOGGER = LoggerFactory.getLogger(ServerConfiguration.class);

	/* master configuration such as */
	public InetAddress ipaddress;
	int nclient;
	int discoveryTimeout;
	int pingTimeout;
	int pingFrequency;
	HashMap<InetAddress, Boolean> neighbors;
	String awsid;
	String awskey;
	private static Map<InetAddress, ClientConfiguration> configMap;
	int reducercount = 1;

	/**
	 * constructor
	 * 
	 * @param nclients
	 */
	public ServerConfiguration(int nclients) {
		nclient = nclients;
		neighbors = new HashMap<InetAddress, Boolean>();
		configMap = new HashMap<InetAddress, ClientConfiguration>();
		ipaddress = NetworkUtils.getBroadcastIpAddress();
	}

	/**
	 * update client to list of connected clients.
	 * 
	 * @param address
	 * @return
	 */
	public boolean updateClient(String address) {
		try {
			if (!configMap.containsKey(InetAddress.getByName(address))) {
				ClientConfiguration clientconfig = new ClientConfiguration();
				clientconfig.setIpaddressbyName(address);
				clientconfig.setActive(false);
				configMap.put(InetAddress.getByName(address), clientconfig);
				return true;
			}

		} catch (UnknownHostException e) {
			LOGGER.error(e.getMessage());
		}

		return false;
	}

	/**
	 * add client observer
	 * 
	 * @param address
	 * @param observer
	 * @return
	 */
	public boolean addObserver(String address, ClientObserver observer) {
		try {
			if (configMap.containsKey(InetAddress.getByName(address))) {
				configMap.get(InetAddress.getByName(address)).setObserver(observer);
				return true;
			}
		} catch (UnknownHostException e) {
			LOGGER.error(e.getMessage());
		}
		return false;
	}

	/**
	 * return client observer
	 * 
	 * @param ipaddress
	 * @return
	 */
	public ClientObserver observedClient(InetAddress ipaddress) {
		return configMap.get(ipaddress).getObserver();
	}

	/**
	 * return number of neighbors.
	 * 
	 * @return
	 */
	public int activeNeighbors() {
		return configMap.size();
	}

	/**
	 * get client configuration form its ipadress
	 * 
	 * @return
	 */
	public Map<InetAddress, ClientConfiguration> getClientConfiguration() {
		return configMap;
	}

	/* get client count */
	public int clientCount() {
		return nclient;
	}

	/* get discovery time */
	public int discoveyTimeout() {
		return discoveryTimeout;
	}

	/* get ping timeout */
	public int pingTimeout() {
		return pingTimeout;
	}

	/* set ping frequency */
	public int pingFrequency() {
		return pingFrequency;
	}

	/**
	 * update receive count.
	 * 
	 * @param count
	 */
	public void setReducerCount(int count) {
		reducercount = count;
	}

	public int reducerCount() {
		return reducercount;
	}

}
