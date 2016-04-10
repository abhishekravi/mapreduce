package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import proj.mapreduce.utils.FileOp;
import proj.mapreduce.utils.awshelper.S3ListKeys;

/**
 * Create a container for dataset properties
 * @author allmembers
 *
 */
public class Dataset {
	
	String m_type;
	String m_accesskey;
	String m_secretkey;
	String m_user;
	String m_password;
	String m_bucketname;
	String m_folder;
	String m_ip;
	int    m_port;
	String m_intermediatefiles;
	
	/**
	 * creae dataset
	 * @param type
	 * @param bucketname
	 * @param accesskey
	 * @param secretkey
	 * @param user
	 * @param password
	 */
	Dataset (String type, String bucketname, String accesskey, String secretkey, String user, String password)
	{
		m_type  =type;
		m_accesskey = accesskey;
		m_secretkey = secretkey;
		m_user = user;
		m_password = password;		
		m_bucketname = bucketname;
	}
	
	/**
	 * dataset constructor
	 * @param type
	 * @param bucketname
	 * @param folder
	 * @param awsid
	 * @param awskey
	 */
	public Dataset(String type, String bucketname, String folder, String awsid, String awskey)
	{
		m_type = type;
		m_bucketname = bucketname;
		m_accesskey = awsid;
		m_secretkey = awskey;
		m_folder = folder;
	}
	
	/**
	 * dataset constructor
	 * @param ip
	 * @param port
	 * @param user
	 * @param pass
	 * @param intermediatefiles
	 */
	public Dataset(String ip, int port, String user, String pass, String intermediatefiles) {
		m_type = "ftp";
		m_ip = ip;
		m_port = port;
		m_user = user;
		m_password = pass;
		m_intermediatefiles = intermediatefiles;
		
	}

	/**
	 * schedule data with in a dataset
	 * @param cclient
	 * @return
	 * @throws IOException
	 */
	public ArrayList<List<String>> distribute(int cclient) throws IOException
	{
		int index = 0;
		Map <String, Long> filelist = new HashMap<String, Long>();
		
		switch (m_type)
		{
		case "aws":
			filelist = S3ListKeys.getbySize(m_bucketname, m_folder, m_accesskey, m_secretkey);
			break;
		case "local":
			filelist = FileOp.getFileBySize(m_bucketname);
			break;
		}
		
		List<Entry<String,Long>> entryList = new ArrayList<Map.Entry<String, Long>>(filelist.entrySet());
		ArrayList<List<String>> chunks = new ArrayList<List<String>>(cclient);
		
		for (int i = 0; i < cclient; i++)
		{
			chunks.add(new ArrayList<String>());
		}
		
		
		while (!entryList.isEmpty())
		{
			chunks.get(index).add(entryList.get(0).getKey());
			entryList.remove(0);
			
			if (entryList.isEmpty())
				break;
			
			chunks.get(index).add(entryList.get(entryList.size()-1).getKey());
			entryList.remove(entryList.size()-1);
			
			index = (index + 1)%cclient;
		}
		
		return chunks;
	}

	/**
	 * get dataset type
	 * @return
	 */
	public String type ()
	{
		return m_type;
	}
	
	/**
	 * get bucket name
	 * @return
	 */
	public String bucketname()
	{
		return m_bucketname;
	}
	
	/**
	 * get ftp configuration
	 * @return
	 */
	public String getFtpConfig ()
	{
		return (m_ip + "," + m_port + "," + m_user + "," + m_password + "," + m_intermediatefiles); 
	}
}