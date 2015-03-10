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
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
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
public class sqsAlertReceive {

    public static void main(String[] args) throws Exception {

        /*
         * The ProfileCredentialsProvider will return your [jreilly]
         * credential profile by reading from the credentials file located at
         * (/Users/johnreilly/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("jreilly").getCredentials();
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
        
		String thisQueue = "reillyInbound001";
		String nextQueue = "alertReceive";

        System.out.println("");
        System.out.println("===========================================");
        System.out.println("Getting Started with sqsAlertReceive");
        System.out.println("===========================================\n");

        try {
            // Create a queue
//            System.out.println("Creating a new SQS queue called MyQueue.\n");
//            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
//            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

            // List queues
            System.out.println("Listing all queues in your account.");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("QueueUrl: " + queueUrl);
            }
            System.out.println();
            
            int x = 1;
            while (x > 0) {

            // Receive messages
            System.out.println("Receiving messages from " + thisQueue + ".");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(thisQueue);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            System.out.println("Message count for " + thisQueue + ": " + 
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
	            
	              // call a function to transform message
	              // then send message to database persist queue
	
		          System.out.println("Sending message to next queue.");
		          sqs.sendMessage(new SendMessageRequest(nextQueue, message.getBody()));
		          
		          // delete message after sending to persist queue
		          System.out.println("Deleting message.\n");
		          String messageRecieptHandle = message.getReceiptHandle();
		          sqs.deleteMessage(new DeleteMessageRequest(thisQueue, messageRecieptHandle));
		          
	            }
	            Thread.sleep(20000); // do nothing for 1000 miliseconds (1 second)
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
}
