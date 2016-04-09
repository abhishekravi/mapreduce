package proj.mapreduce.job;


import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.server.ServerConfiguration;
import proj.mapreduce.utils.KeyPair;


public class JobScheduler {

	static Queue<Job> m_jobs;
	static ServerConfiguration m_serverconf;

	public JobScheduler(ServerConfiguration serverconf) {
		m_jobs = new PriorityQueue<Job>();
		m_serverconf = serverconf;
	}

	
	public void addJob (Job job)
	{
		m_jobs.add(job);
	}

	public KeyPair <Job, ClientConfiguration> scheduleNextJob ()
	{
		KeyPair<Job, ClientConfiguration> pair = null;
		ClientConfiguration client;
		
		Iterator<ClientConfiguration> citr = m_serverconf.getClientConfiguration().values().iterator(); 
		while (citr.hasNext())
		{
			client = (ClientConfiguration) citr.next();

			if (!client.busy())
			{
				pair = new KeyPair<Job, ClientConfiguration> (m_jobs.poll(), client);
				client.updateStatus(true);
				return pair;
			}
		}

		return pair;
	}
	
	public void buid (String jobname, String input, String output, int nclient)
	{
		for (int i = 0; i < nclient; i++)
		{
			String [] str = new String[2];
			str[0] = input;
			str[1] = output;
			
			JobConfiguration jobconf = new JobConfiguration();
			jobconf.setup(jobname, null, str);
			
			Job job = new Job ();
			job.setup(jobconf);
			
			m_jobs.add(job);
		}
		
		/* build merge job */
		JobConfiguration mergjobconf = new JobConfiguration();
		mergjobconf.setup("merge", null, null);
		
		Job mergejob = new Job ();
		mergejob.setup(mergjobconf);
		
		m_jobs.add(mergejob);
		
	}
	
	
}
