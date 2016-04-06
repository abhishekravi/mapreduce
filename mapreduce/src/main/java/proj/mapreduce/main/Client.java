package proj.mapreduce.main;



import java.io.IOException;
import proj.mapreduce.client.ClientManager;

public class Client {
	
	public static void main(String[] args) throws IOException {
		ClientManager.startPinging();
		
		while (ClientManager.busy());
		
	}
}