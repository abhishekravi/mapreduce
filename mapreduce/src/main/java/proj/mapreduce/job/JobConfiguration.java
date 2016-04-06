package proj.mapreduce.job;

public class JobConfiguration {

	String m_jarname;
	String m_mainclass;
	String[] m_args;
	
	public void setup(String jname, String mclass, String[] args)
	{
		m_jarname = jname;
		m_mainclass = mclass;
		m_args = args;
	}
	
	public void setup(String path)
	{
		
	}
	
	public String jobName() {
		return m_jarname;
	}
	
	public String jobMainClass() {
		return m_mainclass;
	}
	
	public String[] jobArgs() {
		return m_args;
	}
}
