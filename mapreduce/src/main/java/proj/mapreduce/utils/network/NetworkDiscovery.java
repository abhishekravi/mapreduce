package proj.mapreduce.utils.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;

import proj.mapreduce.server.Listener;

/**
 * Class to discover networks in the same subnet.
 * 
 * @author all
 *
 */
public class NetworkDiscovery {
	
	private static HashMap<InetAddress, Boolean> 	m_neighbors; 
	private final static int m_port = 54321;
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
			bcsocket = new DatagramSocket(m_port);
			bcsocket.setBroadcast(true);
			bcsocket.connect(InetAddress.getByName("255.255.255.255"), m_port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		 
		 return bcsocket;
	}
	
	private void sendDiscoveryPacket(DatagramSocket socket)
	{
		byte [] buf = Command.YARN_DETECT.toString().getBytes();
		DatagramPacket discovermsg = new DatagramPacket(buf, buf.length);
		
		try {
			socket.send(discovermsg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void listen (DatagramSocket socket)
	{
		final DatagramSocket bcsocket = socket;
		
		Runnable listener = new Runnable () {			
			@Override
            public void run() {
				byte[] buf = new byte [8192];
				DatagramPacket replypacket = new DatagramPacket(buf, buf.length);
				String replymsg;
				
				m_active = true;
				
				while (m_active)
				{
					try {
						bcsocket.receive(replypacket);
						replymsg = replypacket.getData().toString() + ":" + 
								replypacket.getAddress().toString() + ":" + 
								replypacket.getPort();
						
						Listener.takeAction(replymsg);
						
						
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
            }
		};
		
		Thread listenerth = new Thread(null, listener, "balalalala");
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
			
			
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}

