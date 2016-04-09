package proj.mapreduce.job;

import java.util.Iterator;
import java.util.List;

public class Job implements Comparable<Job> {

	private JobConfiguration m_configuration = null;
	private String m_dfstype;
	private String m_bucketname;
	private List<String> m_dataset;
	private int m_clientId = -1;
	
	Job()
	{
		m_configuration = new JobConfiguration();
	}
	
	Job (String jname, String mclass, String[] args)
	{
		m_configuration = new JobConfiguration();
		m_configuration.setup(jname, mclass, args);
	}
	
	Job (String path)
	{
		m_configuration = new JobConfiguration();
		m_configuration.setup(path);
	}
	
	public void setup(String jname, String mclass, String[] args)
	{
		if (m_configuration == null) return;
		
		m_configuration.setup(jname, mclass, args);
	}
	
	public void setup(JobConfiguration jobconf)
	{
		m_configuration = jobconf;
	}
	
	
	public void setup(String path)
	{
		if (m_configuration == null) return;
		
		m_configuration.setup(path);
	}
	
	public void setClientId(int id)
	{
		m_clientId = id;
	}
	
	public void setDfsType (String type)
	{
		m_dfstype = type;
	}
	
	public int getClientId()
	{
		return m_clientId;
	}
	
	public String getJobName()
	{
		return m_configuration.getName();
	}
	
	public String getJobInput()
	{
		return m_configuration.getInput();
	}
	
	public String getJobOutput()
	{
		return m_configuration.getOutput();
	}

	@Override
	public int compareTo(Job o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString()
	{
		/*for aws: jobname:mainclass:aws,bucketname,accesskey,privatekey,keys:args*/
		/*for local: jobname:mainclass:local,path2input,keys:args*/
		/*for ftp: jobname:mainclass:ftp,ftpaddress,port,path2input,keys:args*/
		
		String job = m_configuration.jobName() + "::" + makeInput() + ":";
		
		for (int i = 0; i < m_configuration.jobArgs().length; i++)
		{
			job += m_configuration.jobArgs()[i];
			job += ",";
		}
		
		job = job.substring(0, job.lastIndexOf(","));
		
		return job;
		
	}
	
	private String makeInput ()
	{
		String input = "null";
		switch (m_dfstype) {
		case "aws":
			break;
		case "ftp":
			break;
		case "local":
			
			input = m_dfstype + "," + m_bucketname;
			
			Iterator<String> itr = m_dataset.iterator();
			while (itr.hasNext())
			{
				input += ",";
				input += itr.next();
			}
			
			break;
			
		default:
			input = "null";
			break;
		}
		
		return input;
	}
	
	public void addDataset(String type, String bucketname, List<String> dataset)
	{
		m_dfstype = type;
		m_bucketname = bucketname; 
		m_dataset = dataset;
	}
}
