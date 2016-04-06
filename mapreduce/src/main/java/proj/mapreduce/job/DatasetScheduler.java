package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DatasetScheduler {

	static ArrayList<Dataset> m_dataset;
	static ArrayList<List<String>> m_chunks; 
	
	static int 	m_nclient;
	
	DatasetScheduler ()
	{
		m_dataset = new ArrayList<Dataset>();
	}
	
	public void configure(String confile)
	{
		m_nclient = 8;
	}
	
	static public void schedule () throws IOException
	{
		m_chunks = m_dataset.get(0).distribute(m_nclient);
	}
	
	public void addDataset ()
	{
		String bucketname = "a"; 
		Dataset ds = new Dataset(bucketname);
		m_dataset.add(ds);
	}
	
	public List<String> getChunck (int index)
	{
		return m_chunks.get(index);
	}
}
