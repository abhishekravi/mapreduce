package proj.mapreduce.utils.awshelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//package utils.aws.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;



/*
 * 
 * 
 * 
 */
public class S3Reader {

	AmazonS3 m_s3; /*Amazon web storage Client*/

	public S3Reader ()
	{
		
	}
	
	public S3Reader (BasicAWSCredentials credential)
	{
		m_s3 = new AmazonS3Client(credential);
	}
	
	
	public S3Reader (String accessKey, String secretKey)
	{
		m_s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
	}
	
	public void init(BasicAWSCredentials credential) 
	{	
		m_s3 = new AmazonS3Client(credential);
	}
	
	public void init(String accessKey, String secretKey) 
	{	
		m_s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));	
	}
	
	

	/**
	 * Read a character oriented file from S3 and write it local
	 *
	 * @param bucketName  Name of bucket
	 * @param key         File Name
	 * @throws IOException
	 */
	public void readFromS3(String bucketName, String key) throws IOException 
	{
		S3Object s3object = m_s3.getObject(new GetObjectRequest(
				bucketName, key));
		System.out.println(s3object.getObjectMetadata().getContentType());
		System.out.println(s3object.getObjectMetadata().getContentLength());

		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;
		while((line = reader.readLine()) != null) {
			// can copy the content locally as well
			// using a buffered writer
			System.out.println(line);
		}
	}
}
