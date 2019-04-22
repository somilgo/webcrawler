package db;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DocumentDB {
	static AmazonS3 s3client;
	public static final String tableName = "somil-cis455-crawls";
	public static final String tableKey = "url";

	public static void init() throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [default]
		 * credential profile by reading from the credentials file located at
		 * (~/.aws/credentials).
		 */
		ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (~/.aws/credentials), and is in valid format.",
					e);
		}

		s3client = AmazonS3ClientBuilder
				.standard()
				.withCredentials(credentialsProvider)
				.withRegion(Regions.US_EAST_1)
				.build();

		if(s3client.doesBucketExistV2(tableName)) {
		} else {
			s3client.createBucket(tableName);
		}



	}

	public static void main(String[] args) throws Exception {
		init();
		ContentHashDB.init();
		System.out.println(getDocumentTime("https://yahoo.com/"));
	}

	public static void addDocumentAfterCheck(String url, String document) {
		s3client.putObject(tableName, url, document);
		ContentHashDB.addHash(document);
	}

	public static Date getDocumentTime(String url) {
		try {
			ObjectMetadata omd = s3client.getObjectMetadata(tableName, url);
			return omd.getLastModified();
		} catch (AmazonServiceException e) {
			if (e.getErrorCode().startsWith("404 Not Found"))
				return null;
			else throw e;
		}
	}

	static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is, "UTF-8").useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	public static String getDocumentContent(String url) {
		try {
			S3Object s3o = s3client.getObject(tableName, url);
			InputStream is = s3o.getObjectContent();
			return convertStreamToString(is);
		} catch (AmazonServiceException e) {
			if (e.getErrorCode().startsWith("404 Not Found"))
				return null;
			else throw e;
		}
	}
}
