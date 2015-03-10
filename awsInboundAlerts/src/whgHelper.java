/*
 * Copyright 2010-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (/Users/johnreilly/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class whgHelper {

	public static Map<String, AttributeValue> newAlert(String alertMessageBody) {
		Map<String, AttributeValue> alert = new HashMap<String, AttributeValue>();
		// parse JSON in message body
		
		System.out.println("sourceID: " + String.valueOf(System.currentTimeMillis()));
		System.out.println("something: adt" + String.valueOf(System.currentTimeMillis()));
		System.out.println("alertmessagebody: " + alertMessageBody);		

		Random rn = new Random();
		int source = rn.nextInt(10) + 1;
		
		alert.put("alertId", new AttributeValue(String.valueOf(System.currentTimeMillis())));
		alert.put("alertSourceId", new AttributeValue(String.valueOf(source)));
		alert.put("alertDateTime", new AttributeValue("adt" + String.valueOf(System.currentTimeMillis())));
		alert.put("alertMessageBody", new AttributeValue(alertMessageBody));
		alert.put("alertPersistedDateTime", new AttributeValue().withN(Double.toString(System.currentTimeMillis())));
		return alert;
	}
	

	public static AWSCredentials getCred(String user) {
		/*
		 * The ProfileCredentialsProvider will return your [user]
		 * credential profile by reading from the credentials file located at
		 * (/Users/johnreilly/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider(user).getCredentials();
			
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/Users/johnreilly/.aws/credentials), and is in valid format.",
							e);
		}
		return credentials;
	}
	
	public static AmazonSQS setQueueAccess(AWSCredentials credentials) {
	
		// setup access with SQS, set region
		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);
		return sqs;
	
	}
	
	
	public static void setTable(AmazonDynamoDBClient dynamoDB, String tableName) {
		
		// Create table if it does not exist yet
		if (Tables.doesTableExist(dynamoDB, tableName)) {
			System.out.println("Table " + tableName + " is already ACTIVE");
		} else {
			// Create a table with a primary hash key named 'name', which holds a string
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
					.withKeySchema(new KeySchemaElement().withAttributeName("alertId").withKeyType(KeyType.HASH))
					.withAttributeDefinitions(new AttributeDefinition().withAttributeName("alertId").withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
			TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);

			// Wait for it to become active
			System.out.println("Waiting for " + tableName + " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDB, tableName);
		}
		
	}
	
	public static List<Message> getMessagesFromQueue(String thisQueue, AmazonSQS sqs) {
		
		// get messages from provided queue
		System.out.println("Receiving messages from " + thisQueue + ".");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(thisQueue);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		return messages;

	}
	
	public static void deleteMessageFromQueue(Message message, String thisQueue, AmazonSQS sqs) {

		// delete message from provided queue
		System.out.println("Deleting message.\n");
		String messageRecieptHandle = message.getReceiptHandle();
		sqs.deleteMessage(new DeleteMessageRequest(thisQueue, messageRecieptHandle));
		
	}
	

}
