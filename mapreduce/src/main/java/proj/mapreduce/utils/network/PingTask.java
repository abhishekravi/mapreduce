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

	int m_timeout;
	HashMap<InetAddress, Boolean> 	m_neighbors;
	protected DatagramSocket m_detectsocket = null;
	protected final int m_detectport = 4544;
	
	public PingTask(HashMap<InetAddress, Boolean> neighbors, int timeout) throws SocketException {
		
		m_timeout = timeout; 
		m_neighbors = neighbors;
		
		m_detectsocket = new DatagramSocket(m_detectport);
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

	
	private void DoPing() throws IOException
	{
		Iterator<InetAddress> itr = m_neighbors.keySet().iterator();
		InetAddress neighbor;
		Boolean reached = true;
		
		while(itr.hasNext()) {
			
			/*point of performance improvement with event driven programming */
			neighbor = itr.next();
			reached = neighbor.isReachable(m_timeout);
		
			if (doDetect(neighbor))
			{
				m_neighbors.put(neighbor, reached);
			}
		}
	}
	
	/* send detect packet */
	private boolean doDetect(InetAddress clientaddr) throws IOException
	{
		String detectstr = Command.YARN_DETECT.toString();
		String recvstr;
		DatagramPacket send_packet, recv_packet;

		
		send_packet = new DatagramPacket(detectstr.getBytes(), detectstr.getBytes().length, clientaddr, m_detectport);
        m_detectsocket.send(send_packet);

        recv_packet = new DatagramPacket(detectstr.getBytes(), detectstr.getBytes().length);
        m_detectsocket.receive(recv_packet);

        recvstr = new String(recv_packet.getData(), 0, recv_packet.getLength());
        
        return recvstr.split(":")[2].equals("yes");
	}
}
