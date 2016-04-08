package proj.mapreduce.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import proj.mapreduce.client.CommandListener;

public class ClientObserver implements Runnable {

	private Socket m_clientsocket;
	Thread m_obsrvth; 
	static DataOutputStream m_outstream;
	BufferedReader m_instream;
	boolean m_active =false;
	
	public ClientObserver(ThreadGroup threadgrp, String clientaddr, int obsrvport) {
		
		m_obsrvth = new Thread(this, "Client Observer");
		
		try {
			m_clientsocket = new Socket(clientaddr, obsrvport);
			m_outstream = new DataOutputStream(m_clientsocket.getOutputStream());
			m_instream = new BufferedReader(new InputStreamReader(m_clientsocket.getInputStream()));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void start() throws IOException
	{
		try {
			if (m_active) return; 		
			m_obsrvth.start();
			m_obsrvth.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		
		
		while(m_active){
			try{
				
				sendCommand("yarn:hellomania\n");
				
				reply = m_instream.readLine();
				// Listener.takeAction(reply);
			
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