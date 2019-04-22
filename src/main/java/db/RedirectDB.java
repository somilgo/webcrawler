package db;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RedirectDB {
	static AmazonDynamoDB dynamoDB;
	public static final String tableName = "redirects";
	public static final String tableKey = "from";

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
				.withKeySchema(new KeySchemaElement().withAttributeName(tableKey).withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName(tableKey).withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

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
	}

	private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("name", new AttributeValue(name));
		item.put("year", new AttributeValue().withN(Integer.toString(year)));
		item.put("rating", new AttributeValue(rating));
		item.put("fans", new AttributeValue().withSS(fans));

		return item;
	}

	public static String getRedirectURL(String from) {
		HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("from", new AttributeValue(from));
		GetItemResult result = dynamoDB.getItem(tableName, item);
		if (result.getItem()==null) {
			return null;
		} else {
			return result.getItem().get("from").getS();
		}
	}

	public static void addRedirectURL(String from, String to) {
		HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("from", new AttributeValue(from));
		item.put("to", new AttributeValue(to));

		PutItemResult result = dynamoDB.putItem(new PutItemRequest(tableName, item));
	}
}
