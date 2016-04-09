package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import proj.mapreduce.utils.FileOp;
import proj.mapreduce.utils.awshelper.S3ListKeys;

public class Dataset {
	
	String m_type;
	String m_accesskey;
	String m_secretkey;
	String m_user;
	String m_password;
	String m_bucketname;
	
	Dataset (String type, String bucketname, String accesskey, String secretkey, String user, String password)
	{
		m_type  =type;
		m_accesskey = accesskey;
		m_secretkey = secretkey;
		m_user = user;
		m_password = password;		
		m_bucketname = bucketname;
	}
	
	public Dataset(String type, String bucketname)
	{
		m_type = type;
		m_bucketname = bucketname;
	}
	
	public ArrayList<List<String>> distribute(int cclient) throws IOException
	{
		int index = 0;
		Map <String, Long> filelist = new HashMap<String, Long>();
		
		switch (m_type)
		{
		case "aws":
			filelist = S3ListKeys.getbySize(m_bucketname);
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
			
			chunks.get(index).add(entryList.get(entryList.size()-1).getKey());
			entryList.remove(entryList.size()-1);
			
			index = (index + 1)%cclient;
		}
		
		return chunks;
	}

	public String type ()
	{
		return m_type;
	}
	
	public String bucketname()
	{
		return m_bucketname;
	}
}
