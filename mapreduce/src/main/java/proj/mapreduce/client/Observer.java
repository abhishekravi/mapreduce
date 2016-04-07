package proj.mapreduce.client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Observer implements Runnable {

	Thread m_observeth;
	boolean m_active = false;
	int m_port = 6789;
	InetAddress m_serverip;
	Socket m_serversocket;
	static DataOutputStream m_outstream;
	BufferedReader m_instream;
	
	
	Observer (InetAddress serverip) throws UnknownHostException, IOException
	{
		m_serverip = serverip;
	}

	public void start() throws UnknownHostException, IOException
	{
		if (m_active) return;
		
		setupConnection();
		createObserveTh();		
	}
	
	public void stop() throws IOException
	{
		m_active = false;
		m_outstream.close();
		m_instream.close();
		m_serversocket.close(); 
	}
	
	private void setupConnection() throws UnknownHostException, IOException {

		m_serversocket = new Socket(m_serverip, m_port);
		m_outstream = new DataOutputStream(m_serversocket.getOutputStream());
		m_instream = new BufferedReader(new InputStreamReader(m_serversocket.getInputStream()));
	}

	private void createObserveTh() 
	{
		m_observeth = new Thread(this, "observation thread");
		m_observeth.start();		
	}


	@Override
	public void run() {
		m_active = true;
		String command;
		while (m_active)
		{
			
			try {
				
				command = m_instream.readLine();
				CommandListener.takeAction(command);
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	public static void updateServer(String reply) throws IOException
	{
		m_outstream.writeBytes(reply);
	}

}
