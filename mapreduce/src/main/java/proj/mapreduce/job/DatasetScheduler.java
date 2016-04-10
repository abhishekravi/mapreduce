package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import proj.mapreduce.server.ServerConfiguration;

public class DatasetScheduler {

	static ArrayList<Dataset> m_dataset;
	static ArrayList<List<String>> m_chunks; 
	static ServerConfiguration m_serverconf;


	public DatasetScheduler (ServerConfiguration serverconf)
	{
		m_dataset = new ArrayList<Dataset>();
		m_serverconf = serverconf;
	}


	public void schedule (int index)
	{
		try {
			m_chunks = m_dataset.get(index).distribute(m_serverconf.clientCount());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addDataset (Dataset ds)
	{
		m_dataset.add(ds);
	}

	public List<String> getChunck (int index)
	{
		return m_chunks.get(index);
	}

	public List<String> getNextChunk ()
	{
		List<String> keys = new ArrayList<String>();

		if (m_chunks.size() <= 0) return null;

		keys = m_chunks.get(0);
		m_chunks.remove(0);

		return keys;
	}

	public String getType()
	{
		return m_dataset.get(0).type();
	}

	public String getBucketname()
	{
		return m_dataset.get(0).bucketname();
	}
	
	public void removeDataset(int index)
	{
		m_dataset.remove(index);
	}

	public ArrayList<List<String>> reducerSchedule()
	{
		ArrayList<List<String>> chunk = new ArrayList<List<String>>();
		
		try {
			
			for (int i = 0; i < m_dataset.size(); i++)
			{	
				ArrayList<List<String>> onechunk = m_dataset.get(i).distribute(m_serverconf.reducerCount());
				chunk.add(onechunk.get(0));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return chunk;

	}
}
