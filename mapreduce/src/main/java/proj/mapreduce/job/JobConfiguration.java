package proj.mapreduce.job;

/**
 * A container class for job configuration
 * @author all members
 *
 */
public class JobConfiguration {

	String jarname;
	String mainclass;
	String[] args;
	
	/**
	 * setup job based on parameters
	 * @param jname
	 * @param mclass
	 * @param args
	 */
	public void setup(String jname, String mclass, String[] args)
	{
		jarname = jname;
		mainclass = mclass;
		this.args = args;
	}

	public String getJarname() {
		return jarname;
	}

	public void setJarname(String jarname) {
		this.jarname = jarname;
	}

	public String getMainclass() {
		return mainclass;
	}

	public void setMainclass(String mainclass) {
		this.mainclass = mainclass;
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}
	
	
}