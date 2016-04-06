package proj.mapreduce.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClientObserver implements Runnable {

	private Socket m_clientsocket;
	Thread m_clientobserverth; 
	static DataOutputStream m_outstream;
	BufferedReader m_instream;
	boolean m_active =false;
	
	public ClientObserver(Socket client) {
		this.m_clientsocket = client;

	}

	public void start() throws IOException
	{
		if (m_active) return; 
		
		m_clientobserverth = new Thread(this, "Client Observer");
		m_clientobserverth.start();

		m_outstream = new DataOutputStream(m_clientsocket.getOutputStream());
		m_instream = new BufferedReader(new InputStreamReader(m_clientsocket.getInputStream()));	
	}

	public void stop() throws IOException
	{
		m_active = false;
		
		m_outstream.close();
		m_instream.close();
		m_clientsocket.close();
		
	}
	
	@Override
	public void run() {

		m_active = true;
		String reply;
		
		while(true){
			try{
				reply = m_instream.readLine();
				
				Listener.takeAction(reply);
				
			}catch (IOException e) {
				System.out.println("Read failed");
				System.exit(-1);
			}
		}

	}

	public static void sendCommand(String command) throws IOException
	{
		m_outstream.writeBytes(command);
	}

}
