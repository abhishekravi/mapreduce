package proj.mapreduce.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * TCP Connection to communicate with client
 * @author root
 *
 */
public class ClientObserver implements Runnable {

	private Socket clientsocket;
	Thread obsrvth; 
	static DataOutputStream outstream;
	BufferedReader instream;
	boolean active =false;
	
	/**
	 * constructor based on parameters
	 * @param threadgrp
	 * @param clientaddr
	 * @param obsrvport
	 */
	public ClientObserver(ThreadGroup threadgrp, String clientaddr, int obsrvport) {
		
		obsrvth = new Thread(this, "Client Observer");
		
		try {
			clientsocket = new Socket(clientaddr, obsrvport);
			outstream = new DataOutputStream(clientsocket.getOutputStream());
			instream = new BufferedReader(new InputStreamReader(clientsocket.getInputStream()));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * start TCP listener thread
	 */
	public void start()
	{
		if (active) return; 		
		obsrvth.start();
	}

	/*
	 * stop oberver
	 */
	public void stop() throws IOException
	{
		active = false;
		
		outstream.close();
		instream.close();
		clientsocket.close();
		
	}
	
	/**
	 * therunner class for observer
	 */
	@Override
	public void run() {
		active = true;
	
		String reply;
		
		
		while(active){
			try{		
				reply = instream.readLine();
				
				if (reply != null)
				{
					Listener.takeAction(reply);
				}
				
			
			}catch (IOException e) {
				System.out.println("Read failed");
				System.exit(-1);
			
			}
		}

	}

	public void sendCommand(String command) throws IOException
	{
		outstream.writeBytes(command);
	}

}
