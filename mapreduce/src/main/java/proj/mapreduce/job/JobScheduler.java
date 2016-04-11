package proj.mapreduce.job;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import proj.mapreduce.client.ClientConfiguration;
import proj.mapreduce.server.ServerConfiguration;
import proj.mapreduce.utils.KeyPair;

/**
 * Creates a pool of jobs and scheduling them
 * @author all members
 *
 */
public class JobScheduler {

	static Queue<Job> jobs;
	static ServerConfiguration serverconf;

	/**
	 * constructor 
	 * @param serverconf: server configuration  
	 */
	public JobScheduler(ServerConfiguration serverconf) {
		JobScheduler.jobs = new LinkedList<Job>();
		JobScheduler.serverconf = serverconf;
	}

	/**
	 * add job to the job pool
	 * @param job
	 */
	public void addJob (Job job)
	{
		jobs.add(job);
	}

	/**
	 * gets the first job in the pool and also the first inactive client 
	 * and assign the job to the client
	 * @return a pair of job and client.
	 */
	public KeyPair <Job, ClientConfiguration> scheduleNextJob ()
	{
		KeyPair<Job, ClientConfiguration> pair = null;
		ClientConfiguration client;
		
		Iterator<ClientConfiguration> citr = serverconf.getClientConfiguration().values().iterator(); 
		while (citr.hasNext())
		{
			client = (ClientConfiguration) citr.next();

			if (!client.isActive())
			{
				pair = new KeyPair<Job, ClientConfiguration> (jobs.poll(), client);
				client.setActive(true);
				return pair;
			}
		}

		return pair;
	}
	
	/**
	 * build the job pool based on jobnames. 
	 * @param jobname: name of job
	 * @param input: job args
	 * @param output: job args
	 * @param nclient: number of clients 
	 */
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
			
			jobs.add(job);
		}
		
		/* build merge job */
		JobConfiguration mergjobconf = new JobConfiguration();
		mergjobconf.setup("merge", null, null);
		
		Job mergejob = new Job ();
		mergejob.setup(mergjobconf);
		
		jobs.add(mergejob);
		
	}
	
	
}