package proj.mapreduce.utils.awshelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3ListKeys {

	public static HashMap <String, Long> get (String bucketName) throws IOException {

		ObjectListing objectListing;
		ListObjectsRequest lsObjRequest;

		AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
		HashMap <String, Long> keys = new HashMap<String, Long>();

		try {
			lsObjRequest = new ListObjectsRequest()
					.withBucketName(bucketName);

			do {
				objectListing = s3client.listObjects(lsObjRequest);
				for (S3ObjectSummary objectSummary : 
					objectListing.getObjectSummaries()) {
					keys.put(objectSummary.getKey(), objectSummary.getSize());
				}
				lsObjRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());

		} catch (AmazonServiceException ase) {

			System.out.println("Caught an AmazonServiceException, " +
					"which means your request made it " +
					"to Amazon S3, but was rejected with an error response " +
					"for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());

		} catch (AmazonClientException ace) {

			System.out.println("Caught an AmazonClientException, " +
					"which means the client encountered " +
					"an internal error while trying to communicate" +
					" with S3, " +
					"such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

		return keys;

	}

	public static LinkedHashMap<String, Long> getbySize (String bucketname) throws IOException {

		LinkedHashMap<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		HashMap<String, Long> keylist = get(bucketname);

		List<Long> values = new ArrayList<Long>(keylist.values());
		List<String> keys   = new ArrayList<String>(keylist.keySet());
		Collections.sort(values);

		Iterator<Long> vitr = values.iterator();

		while (vitr.hasNext()) {

			Long testval = vitr.next();
			
			Iterator<String> kitr = keys.iterator();
			while (kitr.hasNext()) {
				
				String testkey = kitr.next();
				
				if (testval == keylist.get(testkey))
				{
					keylist.remove(testkey);
					keys.remove(testkey);
					sortedMap.put(testkey, testval);
				}

			}

		}
		return sortedMap;
	}
}