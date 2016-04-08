package proj.mapreduce.client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class MasterObserver implements Runnable {

	Thread m_observeth;
	boolean m_active = false;
	int m_port = 6789;
	InetAddress m_serverip;
	ServerSocket m_serversocket;
	Socket		 m_clientsocket;
	static DataOutputStream m_outstream;
	BufferedReader m_instream;


	MasterObserver (InetAddress serverip) throws UnknownHostException, IOException
	{
		m_serverip = serverip;
	}

	public void start() throws UnknownHostException, IOException
	{
		if (m_active) return;

		m_serversocket = new ServerSocket(ClientConfiguration.observerPort()); 
		createObserveTh();		
	}

	public void stop() throws IOException
	{
		m_active = false;
		m_outstream.close();
		m_instream.close();
		m_serversocket.close(); 
	}

	private void setupConnection() throws UnknownHostException, IOException 
	{
		m_clientsocket = m_serversocket.accept(); 
		m_outstream = new DataOutputStream(m_clientsocket.getOutputStream());
		m_instream = new BufferedReader(new InputStreamReader(m_clientsocket.getInputStream()));
	}

	private void createObserveTh() 
	{
		m_observeth = new Thread(this, "observation thread");
		m_observeth.start();		
	}


	@Override
	public void run() 
	{
		m_active = true;
		String command;

		try {
			setupConnection();
			while (m_active)
			{				
				command = m_instream.readLine();
				
				updateServer(command + ":responcemania\n");

			}

		} catch (IOException e1) {}

	}

	public static void updateServer(String reply) throws IOException
	{
		m_outstream.writeBytes(reply);
	}

}