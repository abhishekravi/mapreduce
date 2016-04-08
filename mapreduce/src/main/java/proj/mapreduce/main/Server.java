package proj.mapreduce.main;

import java.io.IOException;

import proj.mapreduce.server.ServerManager;

public class Server {

	public static void main(String[] args) {
				
		ServerManager m_server = null;
		try {
			m_server = new ServerManager();
			m_server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		while (m_server.busy());

	}

}
