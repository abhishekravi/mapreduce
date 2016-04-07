package proj.mapreduce.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class PingTask implements Runnable {
	
	Thread m_pingth;
	protected static DatagramSocket m_socket = null;
	private final int discoveryport = 54321;
	boolean m_active = false;
	static InetAddress m_serverip; 
	
	
	public void start () throws SocketException
	{
		if (m_active) return; 
		
		setupConnection();
		createListenerTh();
	}
	
	public void stop ()
	{
		if (!m_active) return;
		
		m_active = false;
		m_socket.close();
	}
	
	private void setupConnection() throws SocketException {
		
		m_socket = new DatagramSocket(discoveryport);
	}

	private void createListenerTh ()
	{
		m_pingth = new Thread(this, "Ping runnable thread");
	    m_pingth.start();
	}	

	
	public void run() {
		
		DatagramPacket recv_packet;
		byte[] buf = new byte[256];
		String 	recvstr;
		
		m_active = true;
		
		while (m_active)
		{
			/* Read datagram socket*/
			recv_packet = new DatagramPacket(buf, buf.length);
			
	        try {
				m_socket.receive(recv_packet);
				
				recvstr = new String(recv_packet.getData(), 0, recv_packet.getLength());
		        
		        if (recvstr.split(":")[0].equals("yarn"))
		        {
		        	/* notify listener to take action */
		        	recvstr += ":" + recv_packet.getAddress().toString() + ":" + recv_packet.getPort();
		        	CommandListener.takeAction(recvstr);
		        }
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void sendPingReply (String ack) throws IOException
	{
		InetAddress address = InetAddress.getByName(ack.split(",")[0]);
		int port = Integer.parseInt(ack.split(",")[1]);
		String reply = ack.split(",")[2];
		
		DatagramPacket send_packet = new DatagramPacket(reply.getBytes(), reply.getBytes().length, 
				address, port);		
		
		m_socket.send(send_packet);
		
		m_serverip = address;
	}
	
	public InetAddress serverAddress()
	{
		return m_serverip;
	}
	
}
