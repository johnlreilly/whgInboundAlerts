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
public class sqsAlertPersist {


	static AmazonDynamoDBClient dynamoDB;
	
    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [awsReilly]
         * credential profile by reading from the credentials file located at
         * (/Users/johnreilly/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("awsReilly").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/johnreilly/.aws/credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        dynamoDB.setRegion(usEast1);
    }

	public static void main(String[] args) throws Exception {

        init();

		/*
		 * The ProfileCredentialsProvider will return your [awsReilly]
		 * credential profile by reading from the credentials file located at
		 * (/Users/johnreilly/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("awsReilly").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/Users/johnreilly/.aws/credentials), and is in valid format.",
							e);
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);
		String testQueue = "alertPersist";

		System.out.println("");
		System.out.println("===========================================");
		System.out.println("Getting Started with sqsAlertPersist");
		System.out.println("===========================================\n");

		try {
			
			System.out.println("dynamodb: " + dynamoDB.toString());
			String tableName = "alerts";

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

			// Receive messages
			System.out.println("Receiving messages from " + testQueue + ".");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(testQueue);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			System.out.println("Message count for " + testQueue + ": " + 
					messages.size() + "\n");

			for (Message message : messages) {

				System.out.println("  Message");
				System.out.println("    MessageId:     " + message.getMessageId());
				System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
				System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
				System.out.println("    Body:          " + message.getBody());
				for (Entry<String, String> entry : message.getAttributes().entrySet()) {
					System.out.println("  Attribute");
					System.out.println("    Name:  " + entry.getKey());
					System.out.println("    Value: " + entry.getValue());
				}
				System.out.println();

				// Add an item to DynamoDB table
				Map<String, AttributeValue> item = newItem(message.getBody());
				PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
				PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
				System.out.println("Result: " + putItemResult);        	

				// then send message to cache queue
				System.out.println("Sending messages to alertsPersist.\n");
				sqs.sendMessage(new SendMessageRequest("alertCache", message.getBody()));

				// delete message after sending to persist queue
				System.out.println("Deleting message.\n");
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(testQueue, messageRecieptHandle));

			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static Map<String, AttributeValue> newItem(String alertMessageBody) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		// parse JSON in message body
		
		System.out.println("sourceID: " + String.valueOf(System.currentTimeMillis()));
		System.out.println("something: adt" + String.valueOf(System.currentTimeMillis()));
		System.out.println("alertmessagebody: " + alertMessageBody);		

		Random rn = new Random();
		int source = rn.nextInt(10) + 1;
		
		item.put("alertId", new AttributeValue(String.valueOf(System.currentTimeMillis())));
		item.put("alertSourceId", new AttributeValue(String.valueOf(source)));
		item.put("alertDateTime", new AttributeValue("adt" + String.valueOf(System.currentTimeMillis())));
		item.put("alertMessageBody", new AttributeValue(alertMessageBody));
		item.put("alertPersistedDateTime", new AttributeValue().withN(Double.toString(System.currentTimeMillis())));
		return item;
	}


}
