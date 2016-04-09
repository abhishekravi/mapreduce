package proj.mapreduce.job;

public class Job {

	private JobConfiguration m_configuration = null;
	
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
}
