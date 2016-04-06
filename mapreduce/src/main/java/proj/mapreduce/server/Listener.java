package proj.mapreduce.server;

import java.io.IOException;
import java.net.ServerSocket;

public class Listener implements Runnable {

	final int m_port = 123456;
	ServerSocket m_serversocket;
	Thread m_threadlistner;
	boolean m_active = false;
	
	Listener() throws IOException
	{
		m_serversocket = new ServerSocket(m_port);
		m_threadlistner = new Thread(this, "Server Listener");
		m_threadlistner.start();
	}
	
	@Override
	public void run() {
		
		m_active = true;
		
		while (m_active)
		{
			ClientObserver clientobserver;
			
			try {
				
				clientobserver = new ClientObserver(m_serversocket.accept());
				clientobserver.start();
				
			} catch (IOException e) {
				e.printStackTrace();
			}

		}	
	}

	/* Parse Input Command and take actoin */
	public static void takeAction(String command)
	{
			
	}
	

}
