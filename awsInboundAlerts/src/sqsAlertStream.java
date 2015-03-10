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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
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
public class sqsAlertStream {

	private static AmazonKinesisClient kinesis;

	public static void main(String[] args) throws Exception {

		// get credentials
		String user = "jreilly";
		AWSCredentials credentials = whgHelper.getCred(user);

		// use credentials to set access to SQS
		AmazonSQS sqs = whgHelper.setQueueAccess(credentials);

		// define queue that messages will be retrieved from
		String thisQueue = "alertStream";
		String nextQueue = "alertErrorHandling";

		// set access to stream instance
		kinesis = new AmazonKinesisClient(credentials);

		final String streamName = "alertsStream";
		final Integer streamSize = 1;

		while (1 > 0) {

			// pull list of current messages (up to 10) in the queue
			List<Message> messages = whgHelper.getMessagesFromQueue(thisQueue, sqs);
			System.out.println("Count of messages in " + thisQueue + ": " + messages.size());

			try {

				for (Message message : messages) {
					
					whgHelper.printMessage(message);
					for (Entry<String, String> entry : message.getAttributes().entrySet()) {
						whgHelper.printMessageEntry(entry);
					}

					// Write record to the stream
					long createTime = System.currentTimeMillis();
					PutRecordRequest putRecordRequest = new PutRecordRequest();
					putRecordRequest.setStreamName(streamName);
					putRecordRequest.setData(ByteBuffer.wrap(String.format(message.getBody(), createTime).getBytes()));
					putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));
					PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
					System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
							putRecordRequest.getPartitionKey(),
							putRecordResult.getShardId(),
							putRecordResult.getSequenceNumber());

					// then send message to cache queue
					System.out.println("Sending messages to next queue.");
					sqs.sendMessage(new SendMessageRequest(nextQueue, message.getBody()));

					// delete message after sending to persist queue
					System.out.println("Deleting message from this queue.\n");
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(thisQueue, messageRecieptHandle));
				}
				Thread.sleep(20000); // do nothing for 1000 miliseconds (1 second)
				
			} catch (AmazonServiceException ase) {
				whgHelper.errorMessagesAse(ase);	
			} catch (AmazonClientException ace) {
				whgHelper.errorMessagesAce(ace);
			}
		}
	}
}
