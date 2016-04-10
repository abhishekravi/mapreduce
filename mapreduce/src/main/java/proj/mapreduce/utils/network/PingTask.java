package proj.mapreduce.utils.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimerTask;

public class PingTask extends TimerTask {

	int timeout;
	HashMap<InetAddress, Boolean> neighbors;
	protected DatagramSocket detectsocket = null;
	protected final int detectport = 4544;

	public PingTask(HashMap<InetAddress, Boolean> neighbors, int timeout) throws SocketException {

		this.timeout = timeout;
		this.neighbors = neighbors;

		detectsocket = new DatagramSocket(detectport);
	}

	@Override
	public void run() {

		System.out.println("Pinging Clients");

		try {
			DoPing();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void DoPing() throws IOException {
		Iterator<InetAddress> itr = neighbors.keySet().iterator();
		InetAddress neighbor;
		Boolean reached = true;

		while (itr.hasNext()) {

			/* point of performance improvement with event driven programming */
			neighbor = itr.next();
			reached = neighbor.isReachable(timeout);

			if (doDetect(neighbor)) {
				neighbors.put(neighbor, reached);
			}
		}
	}

	/* send detect packet */
	private boolean doDetect(InetAddress clientaddr) throws IOException {
		String detectstr = Command.YARN_DETECT.toString();
		String recvstr;
		DatagramPacket send_packet, recv_packet;

		send_packet = new DatagramPacket(detectstr.getBytes(), detectstr.getBytes().length, clientaddr, detectport);
		detectsocket.send(send_packet);

		recv_packet = new DatagramPacket(detectstr.getBytes(), detectstr.getBytes().length);
		detectsocket.receive(recv_packet);

		recvstr = new String(recv_packet.getData(), 0, recv_packet.getLength());

		return recvstr.split(":")[2].equals("yes");
	}
}
