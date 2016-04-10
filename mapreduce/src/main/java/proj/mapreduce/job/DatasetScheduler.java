package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import proj.mapreduce.server.ServerConfiguration;

/**
 * This class create a pool for datasets
 * @author all members
 *
 */
public class DatasetScheduler {

	static ArrayList<Dataset> m_dataset;
	static ArrayList<List<String>> m_chunks; 
	static ServerConfiguration m_serverconf;

	/**
	 * constructor 
	 * @param serverconf
	 */
	public DatasetScheduler (ServerConfiguration serverconf)
	{
		m_dataset = new ArrayList<Dataset>();
		m_serverconf = serverconf;
	}

	/**
	 * schedule data files in a specified dataset
	 * @param index
	 */
	public void schedule (int index)
	{
		try {
			m_chunks = m_dataset.get(index).distribute(m_serverconf.clientCount());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * add dataset to pool
	 * @param ds
	 */
	public void addDataset (Dataset ds)
	{
		m_dataset.add(ds);
	}

	/**
	 * get the scheduled data chunks
	 * @param index
	 * @return
	 */
	public List<String> getChunck (int index)
	{
		return m_chunks.get(index);
	}

	/**
	 * get the first scheduled data chunk
	 * @return
	 */
	public List<String> getNextChunk ()
	{
		List<String> keys = new ArrayList<String>();

		if (m_chunks.size() <= 0) return null;

		keys = m_chunks.get(0);
		m_chunks.remove(0);

		return keys;
	}

	/**
	 * get dataset type
	 * @return
	 */
	public String getType()
	{
		return m_dataset.get(0).type();
	}

	/**
	 * get bucket name
	 * @return
	 */
	public String getBucketname()
	{
		return m_dataset.get(0).bucketname();
	}
	
	/**
	 * remmove dataset from list
	 * @param index
	 */
	public void removeDataset(int index)
	{
		m_dataset.remove(index);
	}

	/**
	 * schedule for reducer
	 * @return
	 */
	public ArrayList<List<String>> reducerSchedule()
	{
		ArrayList<List<String>> chunk = new ArrayList<List<String>>();
		Dataset ds;
		try {
			
			for (int i = 0; i < m_dataset.size(); i++)
			{	
				ds = m_dataset.get(i);
				
				ArrayList<List<String>> onechunk = ds.distribute(m_serverconf.reducerCount());
				onechunk.get(0).add(ds.getFtpConfig());
				chunk.add(onechunk.get(0));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return chunk;

	}
}