package proj.mapreduce.utils.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import proj.mapreduce.server.Listener;
import proj.mapreduce.server.ServerConfiguration;

/**
 * Class to discover networks in the same subnet.
 * 
 * @author all
 *
 */
public class NetworkDiscovery {

	private static HashMap<InetAddress, Boolean> 	m_neighbors; 
	private final static int discoveryport = 54321;
	private static boolean m_active = false;

	public HashMap<InetAddress, Boolean> discover()
	{

		DatagramSocket bcsocket = createDatagramConnection ();
		sendDiscoveryPacket (bcsocket);

		listen (bcsocket);

		return null;
	}

	public HashMap<InetAddress, Boolean> discover(String inet)
	{

		return null;
	}

	private DatagramSocket createDatagramConnection () 
	{
		DatagramSocket bcsocket = null;
		try {
			bcsocket = new DatagramSocket();
			bcsocket.setBroadcast(true);
			//		bcsocket.connect(InetAddress.getByName("10.42.0.255"), m_port);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return bcsocket;
	}

	private void sendDiscoveryPacket(DatagramSocket socket)
	{
		byte [] buf = Command.YARN_DETECT.toString().getBytes();

		try {
			DatagramPacket discovermsg = new DatagramPacket(buf, buf.length, 
					InetAddress.getByName("192.168.1.255"), discoveryport);
			socket.send(discovermsg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void listen (final DatagramSocket socket)
	{		
		Runnable listener = new Runnable () {			
			@Override
			public void run() {

				try {

					//DatagramSocket socket = new DatagramSocket();
					byte[] buf = new byte [256];
					DatagramPacket replypacket = new DatagramPacket(buf, buf.length);
					String replymsg;

					m_active = true;

					while (m_active)
					{
						socket.receive(replypacket);
						replymsg = replypacket.getData().toString() + ":" + 
								replypacket.getAddress().toString() + ":" + 
								replypacket.getPort();

						Listener.takeAction(replymsg);
					}
					
					socket.close();
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};

		Thread listenerth = new Thread(null, listener, "Startup Thread");
		listenerth.start();

		try {
			listenerth.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public static void updateneighbors (String address)
	{
		try {
			m_neighbors.put(InetAddress.getByName(address), true);

			if (m_neighbors.size() >= ServerConfiguration.clientCount())
			{
				m_active = false;
			}

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}

