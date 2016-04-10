package proj.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.server.ServerConfiguration;

/**
 * This class create a pool for datasets
 * 
 * @author all members
 *
 */
public class DatasetScheduler {

	private static Logger LOGGER = LoggerFactory.getLogger(DatasetScheduler.class);
	static ArrayList<Dataset> datasetList;
	static ArrayList<List<String>> chunksList;
	static ServerConfiguration serverconf;

	/**
	 * constructor
	 * 
	 * @param serverconf
	 */
	public DatasetScheduler(ServerConfiguration serverconf) {
		datasetList = new ArrayList<Dataset>();
		DatasetScheduler.serverconf = serverconf;
	}

	/**
	 * schedule data files in a specified dataset
	 * 
	 * @param index
	 */
	public void schedule(int index) {
		try {
			chunksList = datasetList.get(index).distribute(serverconf.clientCount());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * add dataset to pool
	 * 
	 * @param ds
	 */
	public void addDataset(Dataset ds) {
		datasetList.add(ds);
	}

	/**
	 * get the scheduled data chunks
	 * 
	 * @param index
	 * @return
	 */
	public List<String> getChunck(int index) {
		return chunksList.get(index);
	}

	/**
	 * get the first scheduled data chunk
	 * 
	 * @return
	 */
	public List<String> getNextChunk() {
		List<String> keys = new ArrayList<String>();
		if (chunksList.size() <= 0)
			return null;
		keys = chunksList.get(0);
		chunksList.remove(0);

		return keys;
	}

	/**
	 * get dataset type
	 * 
	 * @return
	 */
	public String getType() {
		return datasetList.get(0).getType();
	}

	/**
	 * get bucket name
	 * 
	 * @return
	 */
	public String getBucketname() {
		return datasetList.get(0).getBucketname();
	}

	/**
	 * remmove dataset from list
	 * 
	 * @param index
	 */
	public void removeDataset(int index) {
		datasetList.remove(index);
	}

	/**
	 * schedule for reducer
	 * 
	 * @return
	 */
	public ArrayList<List<String>> reducerSchedule() {
		ArrayList<List<String>> chunk = new ArrayList<List<String>>();
		Dataset ds;
		try {
			for (int i = 0; i < datasetList.size(); i++) {
				ds = datasetList.get(i);
				ArrayList<List<String>> onechunk = ds.distribute(serverconf.reducerCount());
				onechunk.get(0).add(ds.getFtpConfig());
				chunk.add(onechunk.get(0));
			}
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
		return chunk;

	}
}