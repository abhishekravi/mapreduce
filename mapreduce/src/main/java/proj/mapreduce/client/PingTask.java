package proj.mapreduce.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that pings the network.
 * @author root
 *
 */
public class PingTask implements Runnable {
	private static Logger LOGGER = LoggerFactory.getLogger(PingTask.class);
	Thread pingThread;
	protected static DatagramSocket socket = null;
	private final int discoveryport = 54321;
	boolean active = false;
	static InetAddress serverip;

	/**
	 * start the task.
	 * @throws SocketException
	 */
	public void start() throws SocketException {
		if (active)
			return;
		socket = new DatagramSocket(discoveryport);
		pingThread = new Thread(this, "Ping runnable thread");
		pingThread.start();
	}

	/**
	 * 
	 */
	public void stop() {
		if (!active)
			return;
		active = false;
		socket.close();
	}

	/**
	 * 
	 */
	public void run() {

		DatagramPacket recv_packet;
		byte[] buf = new byte[256];
		String recvstr;
		active = true;
		while (active) {
			/* Read datagram socket */
			recv_packet = new DatagramPacket(buf, buf.length);
			try {
				socket.receive(recv_packet);
				recvstr = new String(recv_packet.getData(), 0, recv_packet.getLength());
				if (recvstr.split(":")[0].equals("yarn")) {
					/* notify listener to take action */
					recvstr += ":" + recv_packet.getAddress().toString().replaceFirst("/", "") + ":"
							+ recv_packet.getPort();
					CommandListener.takeAction(recvstr);
				}
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		}
	}

	/**
	 * 
	 * @param ack
	 * @throws IOException
	 */
	public static void sendPingReply(String ack) throws IOException {
		InetAddress address = InetAddress.getByName(ack.split(",")[0]);
		int port = Integer.parseInt(ack.split(",")[1]);
		String reply = ack.split(",")[2] + ":" + ClientConfiguration.serverport;
		DatagramPacket send_packet = new DatagramPacket(reply.getBytes(), reply.getBytes().length, address, port);
		socket.send(send_packet);
		serverip = address;
	}

	/**
	 * 
	 * @return
	 */
	public InetAddress serverAddress() {
		return serverip;
	}

}
