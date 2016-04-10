package proj.mapreduce.job;

import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author all team
 *
 */
public class Job implements Comparable<Job> {

	private JobConfiguration m_configuration = null;
	private String m_dfstype;
	private String m_bucketname;
	private List<String> m_dataset;
	private int m_clientId = -1;
	
	/**
	 * constructor
	 */
	Job()
	{
		m_configuration = new JobConfiguration();
	}
	
	/**
	 * create a job by parameters
	 * @param jname
	 * @param mclass
	 * @param args
	 */
	Job (String jname, String mclass, String[] args)
	{
		m_configuration = new JobConfiguration();
		m_configuration.setup(jname, mclass, args);
	}
	
	/**
	 * creat ajob form a configurtion file
	 * @param path
	 */
	Job (String path)
	{
		m_configuration = new JobConfiguration();
		m_configuration.setup(path);
	}
	
	/**
	 * set up job by parameters
	 * @param jname
	 * @param mclass
	 * @param args
	 */
	public void setup(String jname, String mclass, String[] args)
	{
		if (m_configuration == null) return;
		
		m_configuration.setup(jname, mclass, args);
	}
	
	/**
	 * setup up job from a job configuration
	 * @param jobconf
	 */
	public void setup(JobConfiguration jobconf)
	{
		m_configuration = jobconf;
	}
	
	/**
	 * setup job form a specific configuration file
	 * @param path
	 */
	public void setup(String path)
	{
		if (m_configuration == null) return;
		
		m_configuration.setup(path);
	}
	
	/**
	 * set the client that job is going to run in
	 * @param id
	 */
	public void setClientId(int id)
	{
		m_clientId = id;
	}
	
	/**
	 * get the file system
	 * @param type
	 */
	public void setDfsType (String type)
	{
		m_dfstype = type;
	}
	
	/**
	 * get client id
	 * @return
	 */
	public int getClientId()
	{
		return m_clientId;
	}
	
	/**
	 * get job name
	 * @return
	 */
	public String getJobName()
	{
		return m_configuration.getName();
	}
	
	/**
	 * get job input
	 * @return
	 */
	public String getJobInput()
	{
		return m_configuration.getInput();
	}
	
	/**
	 * get job output arg
	 */
	public String getJobOutput()
	{
		return m_configuration.getOutput();
	}

	/** 
	 * prepare it to be comparable
	 */
	@Override
	public int compareTo(Job o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * create an string for the job instance
	 */
	@Override
	public String toString()
	{
		/*for aws: jobname:mainclass:aws,bucketname,keys:args*/
		/*for local: jobname:mainclass:local,path2input,keys:args*/
		/*for ftp: jobname:mainclass:ftp,ftpaddress,port,path2input,keys:args*/
		
		String job = m_configuration.jobName() + ":class:" + makeInput() + ":";
		
		for (int i = 0; i < m_configuration.jobArgs().length; i++)
		{
			job += m_configuration.jobArgs()[i];
			job += ":";
		}
		return job;
		
	}
	
	/**
	 * map inputs to string
	 * @return
	 */
	private String makeInput ()
	{
		Iterator<String> itr = m_dataset.iterator();
		int i =0;
		String input = "null";
		switch (m_dfstype) {
		case "aws":
			input = m_dfstype + ":" + m_bucketname+":";
			while (itr.hasNext())
			{
				input += itr.next();
				input+= ",";
				i++;
				if(i==2)
					break;
			}
			input = input.substring(0, input.lastIndexOf(","));
			break;
		case "ftp":
			break;
		case "local":
			
			input = m_dfstype + "," + m_bucketname;
			while (itr.hasNext())
			{
				input += itr.next();
				input+= ",";
			}
			input = input.substring(0, input.lastIndexOf(","));
			
			break;
			
		default:
			input = "null";
			break;
		}
		
		return input;
	}
	
	/**
	 * add dataset to job
	 * @param type
	 * @param bucketname
	 * @param dataset
	 */
	public void addDataset(String type, String bucketname, List<String> dataset)
	{
		m_dfstype = type;
		m_bucketname = bucketname; 
		m_dataset = dataset;
	}
}