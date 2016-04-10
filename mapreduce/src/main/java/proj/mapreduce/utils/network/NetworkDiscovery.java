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

	private static Timer disctimer;
	private static HashMap<InetAddress, Boolean> neighbors;
	private static int discoveryport = 54321;
	private static boolean active = false;
	private static DatagramSocket bcsocket;
	private static ServerConfiguration serverconf;

	public NetworkDiscovery(ServerConfiguration serverconf) {
		NetworkDiscovery.serverconf = serverconf;
	}

	public void discover() {
		disctimer = new Timer();
		bcsocket = createDatagramConnection();

		sendDiscoveryPacket();
		listen();

	}

	public HashMap<InetAddress, Boolean> discover(String inet) {

		return null;
	}

	private DatagramSocket createDatagramConnection() {
		DatagramSocket bcsocket = null;
		try {
			bcsocket = new DatagramSocket();
			bcsocket.setBroadcast(true);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return bcsocket;
	}

	private void sendDiscoveryPacket() {
		byte[] buf = Command.YARN_DETECT.toString().getBytes();

		// while (m_active)
		{
			try {

				DatagramPacket discovermsg = new DatagramPacket(buf, buf.length, serverconf.ipaddress, discoveryport);
				bcsocket.send(discovermsg);

				disctimer.schedule(new TimerTask() {
					@Override
					public void run() {

					}
				}, serverconf.discoveyTimeout());

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void listen() {
		Runnable listener = new Runnable() {
			@Override
			public void run() {

				try {

					// DatagramSocket socket = new DatagramSocket();

					byte[] buf = new byte[256];
					DatagramPacket replypacket = new DatagramPacket(buf, buf.length);
					String replymsg;

					active = true;

					while (active) {
						bcsocket.receive(replypacket);

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

		/*
		 * try { //listenerth.join(); } catch (InterruptedException e) {
		 * e.printStackTrace(); }
		 */

	}

	public static void stop() {
		active = false;
		bcsocket.close();
		disctimer.cancel();
	}
}
