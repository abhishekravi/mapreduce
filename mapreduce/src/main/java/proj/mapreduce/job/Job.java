package proj.mapreduce.job;

import java.util.Iterator;
import java.util.List;

/**
 * Job class.
 * 
 * @author all team
 *
 */
public class Job implements Comparable<Job> {

	private JobConfiguration configuration = null;
	private String mode;
	private String bucket;
	private List<String> dataset;
	private int clientId = -1;

	/**
	 * constructor
	 */
	Job() {
		configuration = new JobConfiguration();
	}

	/**
	 * create a job by parameters
	 * 
	 * @param jname
	 * @param mclass
	 * @param args
	 */
	Job(String jname, String mclass, String[] args) {
		configuration = new JobConfiguration();
		configuration.setup(jname, mclass, args);
	}

	/**
	 * creat ajob form a configurtion file
	 * 
	 * @param path
	 */
	Job(String path) {
		configuration = new JobConfiguration();
	}

	/**
	 * set up job by parameters
	 * 
	 * @param jname
	 * @param mclass
	 * @param args
	 */
	public void setup(String jname, String mclass, String[] args) {
		if (configuration == null)
			return;
		configuration.setup(jname, mclass, args);
	}

	/**
	 * setup up job from a job configuration
	 * 
	 * @param jobconf
	 */
	public void setup(JobConfiguration jobconf) {
		configuration = jobconf;
	}

	/**
	 * setup job form a specific configuration file
	 * 
	 * @param path
	 */
	public void setup(String path) {
		if (configuration == null)
			return;
	}

	/**
	 * set the client that job is going to run in
	 * 
	 * @param id
	 */
	public void setClientId(int id) {
		clientId = id;
	}

	/**
	 * get the file system
	 * 
	 * @param type
	 */
	public void setDfsType(String type) {
		mode = type;
	}

	/**
	 * get client id
	 * 
	 * @return
	 */
	public int getClientId() {
		return clientId;
	}

	/**
	 * prepare it to be comparable
	 */
	@Override
	public int compareTo(Job o) {
		return 0;
	}

	/**
	 * create an string for the job instance
	 */
	@Override
	public String toString() {
		/* for aws: jobname:mainclass:aws,bucketname,keys:args */
		/* for local: jobname:mainclass:local,path2input,keys:args */
		/*
		 * for ftp: jobname:mainclass:ftp,ftpaddress,port,path2input,keys:args
		 */
		String job = configuration.getJarname() + ":class:" + makeInput() + ":";
		for (int i = 0; i < configuration.getArgs().length; i++) {
			job += configuration.getArgs()[i];
			job += ":";
		}
		return job;
	}

	/**
	 * map inputs to string
	 * 
	 * @return
	 */
	private String makeInput() {
		Iterator<String> itr = dataset.iterator();
		String input = "null";
		switch (mode) {
		case "aws":
			input = mode + ":" + bucket + ":";
			while (itr.hasNext()) {
				input += itr.next();
				input += ",";
			}
			input = input.substring(0, input.lastIndexOf(","));
			break;
		case "ftp":
			break;
		case "local":
			input = mode + "," + bucket;
			while (itr.hasNext()) {
				input += itr.next();
				input += ",";
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
	 * 
	 * @param type
	 * @param bucketname
	 * @param dataset
	 */
	public void addDataset(String type, String bucketname, List<String> dataset) {
		mode = type;
		bucket = bucketname;
		this.dataset = dataset;
	}
}