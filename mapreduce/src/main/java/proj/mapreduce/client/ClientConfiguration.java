package proj.mapreduce.client;

import java.util.List;

public class ClientConfiguration {

	boolean m_active = false;
	
	public boolean busy()
	{
		return m_active;
	}
	
	public void updateStatus (boolean status)
	{
		m_active = status;
	}
	
	public void assignData(List<String> ds)
	{
		
	}
	
	public static int observerPort ()
	{
		return 9182;
	}
}
