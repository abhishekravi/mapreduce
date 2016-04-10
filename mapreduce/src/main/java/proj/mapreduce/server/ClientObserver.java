package proj.mapreduce.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.main.Server;

/**
 * TCP Connection to communicate with client
 * 
 * @author root
 *
 */
public class ClientObserver implements Runnable {

	private static Logger LOGGER = LoggerFactory.getLogger(Server.class);
	private Socket clientsocket;
	Thread observerThread;
	static DataOutputStream outstream;
	BufferedReader instream;
	boolean active = false;

	/**
	 * constructor based on parameters
	 * 
	 * @param threadgrp
	 * @param clientaddr
	 * @param obsrvport
	 */
	public ClientObserver(ThreadGroup threadgrp, String clientaddr, int obsrvport) {
		observerThread = new Thread(this, "Client Observer");
		try {
			clientsocket = new Socket(clientaddr, obsrvport);
			outstream = new DataOutputStream(clientsocket.getOutputStream());
			instream = new BufferedReader(new InputStreamReader(clientsocket.getInputStream()));
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * start observer thread
	 */
	public void start() {
		if (active)
			return;
		observerThread.start();
	}

	/**
	 * stop observer thread.
	 * 
	 * @throws IOException
	 */
	public void stop() throws IOException {
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
		while (active) {
			try {
				reply = instream.readLine();
				if (reply != null) {
					Listener.takeAction(reply);
				}
			} catch (IOException e) {
				LOGGER.info("Read failed");
				System.exit(-1);
			}
		}

	}

	/**
	 * method to sent commands.
	 * 
	 * @param command
	 *            command to send
	 * @throws IOException
	 */
	public void sendCommand(String command) throws IOException {
		outstream.writeBytes(command);
	}

}
