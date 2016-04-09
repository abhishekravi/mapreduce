package proj.mapreduce.main;



import java.io.IOException;
import proj.mapreduce.client.ClientManager;

public class Client {
	
	public static void main(String[] args) throws IOException {
		
		
		ClientManager m_client = new ClientManager();
		m_client.startPinging();
		//m_client.startFtpServer();
		while (m_client.busy());
		
		
		
	}
}