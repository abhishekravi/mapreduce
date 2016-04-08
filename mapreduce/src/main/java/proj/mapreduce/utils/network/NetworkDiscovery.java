package proj.mapreduce.utils.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import proj.mapreduce.server.Listener;
import proj.mapreduce.server.ServerConfiguration;

/**
 * Class to discover networks in the same subnet.
 * 
 * @author all
 *
 */
public class NetworkDiscovery {

	private static Timer 	m_disctimer;
	private static HashMap<InetAddress, Boolean> 	m_neighbors; 
	private static int discoveryport = 54321;
	private static boolean m_active = false;
	private static DatagramSocket 	m_bcsocket;

	private static ServerConfiguration m_serverconf;

	public NetworkDiscovery(ServerConfiguration serverconf) {
		m_serverconf = serverconf;
	}

	public void discover()
	{
		m_disctimer = new Timer();
		m_bcsocket = createDatagramConnection ();

		listen ();
		sendDiscoveryPacket ();
		
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
		} catch (Exception e) {
			e.printStackTrace();
		}

		return bcsocket;
	}

	private void sendDiscoveryPacket()
	{
		byte [] buf = Command.YARN_DETECT.toString().getBytes();

		while (m_active)
		{
			try {

				DatagramPacket discovermsg = new DatagramPacket(buf, buf.length, 
						InetAddress.getByName("192.168.1.255"), discoveryport);
				m_bcsocket.send(discovermsg);

				m_disctimer.schedule(new TimerTask() {
					@Override
					public void run() {

					}
				}, m_serverconf.discoveyTimeout());

			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}

	private void listen ()
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
						m_bcsocket.receive(replypacket);
						
						replymsg = new String(replypacket.getData(), 0, replypacket.getLength());
						
						replymsg += ":" + replypacket.getAddress().toString().replaceFirst("/", "");

						Listener.takeAction(replymsg);
					}

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

	public static boolean updateneighbors (String address)
	{
		if (!m_serverconf.updateClient(address))
		{
			return false;
		}

		if (m_serverconf.neighbors().size() >= m_serverconf.clientCount())
		{
//			stop ();
		}

		return true;
	}

	public static void stop ()
	{
		m_active = false;
		m_bcsocket.close();
		m_disctimer.cancel();
	}
}

