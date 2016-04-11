package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.jcraft.jsch.Logger;

import java.util.Set;

import proj.mapreduce.utils.FileOp;
import proj.mapreduce.utils.awshelper.S3ListKeys;

/**
 * Create a container for dataset properties
 * 
 * @author allmembers
 *
 */
public class Dataset {

	String type;
	String accesskey;
	String secretkey;
	String user;
	String password;
	String bucketname;
	String folder;
	String ip;
	int port;
	String intermediatefiles;

	/**
	 * create dataset
	 * 
	 * @param type
	 * @param bucketname
	 * @param accesskey
	 * @param secretkey
	 * @param user
	 * @param password
	 */
	Dataset(String type, String bucketname, String accesskey, String secretkey, String user, String password) {
		this.type = type;
		this.accesskey = accesskey;
		this.secretkey = secretkey;
		this.user = user;
		this.password = password;
		this.bucketname = bucketname;
	}

	/**
	 * create dataset
	 * 
	 * @param type
	 * @param bucketname
	 * @param folder
	 * @param awsid
	 * @param awskey
	 */
	public Dataset(String type, String bucketname, String folder, String awsid, String awskey) {
		this.type = type;
		this.bucketname = bucketname;
		accesskey = awsid;
		secretkey = awskey;
		this.folder = folder;
	}

	/**
	 * create dataset
	 * 
	 * @param ip
	 * @param port
	 * @param user
	 * @param pass
	 * @param intermediatefiles
	 */
	public Dataset(String ip, int port, String user, String pass, String intermediatefiles) {
		type = "ftp";
		this.ip = ip;
		this.port = port;
		this.user = user;
		this.password = pass;
		this.intermediatefiles = intermediatefiles;

	}

	/**
	 * schedule data with in a dataset
	 * 
	 * @param cclient
	 * number of clients
	 * @return
	 * @throws IOException
	 */
	public ArrayList<List<String>> distribute(int cclient) throws IOException {
		Map<String, Long> filelist = new HashMap<String, Long>();

		switch (type) {
		case "aws":
			filelist = S3ListKeys.getbySize(bucketname, folder, accesskey, secretkey);
			break;
		case "local":
			filelist = FileOp.getFileBySize(bucketname);
			break;
		}

		ArrayList<List<String>> chunks = new ArrayList<List<String>>(cclient);

		for (int i = 0; i < cclient; i++) {
			chunks.add(new ArrayList<String>());
		}

		Iterator<String> it = filelist.keySet().iterator();
		for (int i = 0; i < filelist.size(); i++) {
			chunks.get(i % cclient).add(it.next());
		}

		return chunks;
	}

	/**
	 * get dataset type
	 * 
	 * @return
	 */
	public String getType() {
		return type;
	}

	/**
	 * get bucket name
	 * 
	 * @return
	 */
	public String getBucketname() {
		return bucketname;
	}

	/**
	 * get ftp configuration
	 * 
	 * @return
	 */
	public String getFtpConfig() {
		return (ip + "," + port + "," + user + "," + password + "," + intermediatefiles);
	}
}