package proj.mapreduce.job;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.server.ServerConfiguration;
import proj.mapreduce.utils.KeyPair;


public class JobScheduler {

	static Queue<Job> m_jobs;
	static List <ClientConfiguration> m_clientconf;
	static DatasetScheduler m_dssched;

	public JobScheduler() {
		m_jobs = new PriorityQueue<Job>();
		m_clientconf = new ArrayList<ClientConfiguration>();
	}

	public void addJob (Job job)
	{
		m_jobs.add(job);
	}

	public void addClient (ClientConfiguration client)
	{
		m_clientconf.add(client);
	}

	public KeyPair <Job, ClientConfiguration> scheduleNextJob ()
	{
		KeyPair<Job, ClientConfiguration> pair = null;
		ClientConfiguration client;

		Iterator<ClientConfiguration> citr = m_clientconf.iterator(); 
		while (citr.hasNext())
		{
			client = (ClientConfiguration) citr.next();

			if (!client.busy())
			{
				client.assignData (m_dssched.getChunck(0));
				pair = new KeyPair<Job, ClientConfiguration> (m_jobs.poll(), client);
				client.updateStatus(true);
				return pair;
			}
		}

		return pair;
	}
	
	public void buid (String jobname, int nclient)
	{
		for (int i = 0; i < nclient; i++)
		{
			JobConfiguration jobconf = new JobConfiguration();
			jobconf.setup(jobname, null, null);
			
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
