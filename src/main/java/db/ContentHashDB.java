package db;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import javax.xml.bind.DatatypeConverter;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class ContentHashDB {

	static AmazonDynamoDB dynamoDB;
	public static final String tableName = "content_seen2";

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
		dynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName("hashed_content").withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName("hashed_content").withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

		DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withTableName(tableName);

		// Create table if it does not exist yet
		TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
		// wait for the table to move into ACTIVE state
		TableUtils.waitUntilActive(dynamoDB, tableName);

		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);

	}

	public static void main(String[] args) throws Exception {
		init();
		addHash("yolo");
		System.out.println(contains("yolo"));
		System.out.println(contains("different sgring"));
	}

	private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("name", new AttributeValue(name));
		item.put("year", new AttributeValue().withN(Integer.toString(year)));
		item.put("rating", new AttributeValue(rating));
		item.put("fans", new AttributeValue().withSS(fans));

		return item;
	}

	public static boolean contains(String content) {
		MessageDigest messageDigest = null;
		try { messageDigest = MessageDigest.getInstance("MD5"); }
		catch (NoSuchAlgorithmException e) {}

		messageDigest.update(content.getBytes());
		byte[] digiest = messageDigest.digest();
		String hashedOutput = DatatypeConverter.printHexBinary(digiest);

		HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("hashed_content", new AttributeValue(hashedOutput));

		GetItemResult result = dynamoDB.getItem(tableName, item);

		return result.getItem() != null;
	}

	public static boolean addHash(String content) {
		MessageDigest messageDigest = null;
		try { messageDigest = MessageDigest.getInstance("MD5"); }
		catch (NoSuchAlgorithmException e) {}

		messageDigest.update(content.getBytes());
		byte[] digiest = messageDigest.digest();
		String hashedOutput = DatatypeConverter.printHexBinary(digiest);

		HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("hashed_content", new AttributeValue(hashedOutput));

		PutItemResult result = dynamoDB.putItem(new PutItemRequest(tableName, item));

		return result != null;
	}
}