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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
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

	public static void main(String[] args) throws Exception {

		// get credentials
		String user = "jreilly";
		AWSCredentials credentials = whgHelper.getCred(user);

		// use credentials to set access to SQS
		AmazonSQS sqs = whgHelper.setQueueAccess(credentials);

		// define queue that messages will be retrieved from
		String thisQueue = "alertPersist";
		String nextQueue = "alertCache";

		// set access to database with credentials
		dynamoDB = new AmazonDynamoDBClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		dynamoDB.setRegion(usEast1);

		// check for table, create one if missing
		String tableName = "alerts";
		whgHelper.setTable(dynamoDB, tableName);

		while (1 > 0) {

			// pull list of current messages (up to 10) in the queue
			List<Message> messages = whgHelper.getMessagesFromQueue(thisQueue, sqs);
			System.out.println("Count of messages in " + thisQueue + ": " + messages.size());

			try {

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

					// Add an item to DynamoDB table
					Map<String, AttributeValue> item = whgHelper.newAlert(message.getBody());
					PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
					PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
					System.out.println();
					System.out.println("Result: " + putItemResult);        	

					// then send message to cache queue
					System.out.println("Sending messages to next queue.");
					sqs.sendMessage(new SendMessageRequest(nextQueue, message.getBody()));

					// delete message after sending to persist queue
					System.out.println("Deleting message from this queue.\n");
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(thisQueue, messageRecieptHandle));
					
				}
				Thread.sleep(20000); // do nothing for 2000 miliseconds (2 second)
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
	}
}


