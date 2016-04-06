package proj.mapreduce.main;

import proj.mapreduce.server.ServerManager;

public class Server {

	public static void main(String[] args) {
				
		ServerManager m_server = new ServerManager();
		m_server.start();
		
		while (m_server.busy());

	}

}
