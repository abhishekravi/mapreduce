package proj.mapreduce.main;



import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import proj.mapreduce.client.ClientManager;

public class Client {
	
	public static void main(String[] args) throws IOException {
		Properties prop = new Properties();
		InputStream input;
		String filename = "config.properties";
		input = Client.class.getClassLoader().getResourceAsStream(filename);
		prop.load(input);
		ClientManager m_client = new ClientManager(prop.getProperty("awsid"), prop.getProperty("awskey"));
		m_client.startPinging();
		//m_client.startFtpServer();
		while (m_client.busy());
		
		
		
	}
}