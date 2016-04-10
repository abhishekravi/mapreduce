package proj.mapreduce.client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * observer class that communicates with master.
 * 
 * @author root
 *
 */
public class MasterObserver implements Runnable {

	Thread observer;
	boolean active = false;
	int port = 6789;
	InetAddress serverip;
	ServerSocket serversocket;
	Socket clientsocket;
	static DataOutputStream outstream;
	BufferedReader instream;

	/**
	 * setting address of observer.
	 * 
	 * @param serverip
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	MasterObserver(InetAddress serverip) throws UnknownHostException, IOException {
		this.serverip = serverip;
	}

	/**
	 * start the observer
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void start() throws UnknownHostException, IOException {
		if (active)
			return;
		serversocket = new ServerSocket(ClientConfiguration.serverport);
		observer = new Thread(this, "observation thread");
		observer.start();
	}

	/**
	 * stop the observer.
	 * 
	 * @throws IOException
	 */
	public void stop() throws IOException {
		active = false;
		outstream.close();
		instream.close();
		serversocket.close();
	}

	/**
	 * setup the connection.
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	private void setupConnection() throws UnknownHostException, IOException {
		clientsocket = serversocket.accept();
		outstream = new DataOutputStream(clientsocket.getOutputStream());
		instream = new BufferedReader(new InputStreamReader(clientsocket.getInputStream()));
	}

	/**
	 * 
	 */
	@Override
	public void run() {
		active = true;
		String command;
		try {
			setupConnection();
			while (active) {
				command = instream.readLine();
				CommandListener.takeAction(command);
			}

		} catch (IOException e1) {
		}

	}

	/**
	 * send update to server.
	 * 
	 * @param reply
	 * @throws IOException
	 */
	public static void updateServer(String reply) throws IOException {
		outstream.writeBytes(reply);
	}

}
