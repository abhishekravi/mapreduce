package proj.mapreduce.utils.awshelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3ListKeys {

	public static HashMap<String, Long> get(String bucketName, String folder, String awsid, String awskey)
			throws IOException {

		ObjectListing objectListing;
		ListObjectsRequest lsObjRequest;

		AmazonS3 s3client = new AmazonS3Client(new BasicAWSCredentials(awsid, awskey));
		HashMap<String, Long> keys = new HashMap<String, Long>();

		try {
			lsObjRequest = new ListObjectsRequest().withBucketName(bucketName);

			do {
				objectListing = s3client.listObjects(lsObjRequest);
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					if (objectSummary.getKey().contains(folder)){
						if( !objectSummary.getKey().equals(folder+"/"))
							keys.put(objectSummary.getKey(), objectSummary.getSize());
					}
				}
				lsObjRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {

			System.out.println("Caught an AmazonServiceException, " + "which means your request made it "
					+ "to Amazon S3, but was rejected with an error response " + "for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());

		} catch (AmazonClientException ace) {

			System.out.println("Caught an AmazonClientException, " + "which means the client encountered "
					+ "an internal error while trying to communicate" + " with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

		return keys;

	}

	public static HashMap<String, Long> getbySize(String bucketname, String folder, String awsid, String awskey)
			throws IOException {
		return (HashMap<String, Long>) sortByValue(get(bucketname, folder, awsid, awskey));
	}

	/**
	 * sort hash map
	 * @param map
	 * @return
	 */
	private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}