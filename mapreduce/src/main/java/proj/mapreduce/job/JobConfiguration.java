package proj.mapreduce.job;

/**
 * A container class for job configuration
 * @author all members
 *
 */
public class JobConfiguration {

	String m_jarname;
	String m_mainclass;
	String[] m_args;
	
	/**
	 * setup job based on parameters
	 * @param jname
	 * @param mclass
	 * @param args
	 */
	public void setup(String jname, String mclass, String[] args)
	{
		m_jarname = jname;
		m_mainclass = mclass;
		m_args = args;
	}
	
	/**
	 * make job by reading from configuration file 
	 * @param path
	 */
	public void setup(String path)
	{
		
	}
	
	/**
	 * set up job args
	 * @param args
	 */
	public void setArgs (String[] args)
	{
		m_args = args;
	}
	
	/**
	 * get jobname
	 * @return
	 */
	
	public String jobName() {
		return m_jarname;
	}
	
	/**
	 * get job main class
	 * @return
	 */
	public String jobMainClass() {
		return m_mainclass;
	}
	
	/**
	 * get job args
	 * @return
	 */
	public String[] jobArgs() {
		return m_args;
	}
	
	/**
	 * get job name
	 * @return
	 */
	public String getName()
	{
		return m_jarname;
	}
	
	/**
	 * get input
	 * @return
	 */
	public String getInput()
	{
		return m_args[0];
	}
	
	/**
	 * get output
	 * @return
	 */
	public String getOutput()
	{
		return m_args[1];
	}
}