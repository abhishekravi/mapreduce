package proj.mapreduce.main;

import java.io.IOException;

import proj.mapreduce.server.ServerManager;

public class Server {

	public static void main(String[] args) {
				
		ServerManager m_server = null;
		
		if (args.length < 3)
		{
			// 0: name of application
			// 1: number of clients
			// 2: nameofjob
			// 3: bucket
			return;				
		}
		
		int nclient = Integer.parseInt(args[1]);
		String jobfile = args[2];
		String input   = args[3];
		String output = args[4];
		
		try {
			m_server = new ServerManager(nclient, jobfile, input, output);
			m_server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		while (m_server.busy());

	}

}
