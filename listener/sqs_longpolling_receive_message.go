/*
   Based on the following with modifications:

   Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

   This file is licensed under the Apache License, Version 2.0 (the "License").
   You may not use this file except in compliance with the License. A copy of
   the License is located at

    http://aws.amazon.com/apache2.0/

   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
   CONDITIONS OF ANY KIND, either express or implied. See the License for the
   specific language governing permissions and limitations under the License.
*/

package listener

import (
	"fmt"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Receive message from Queue with long polling enabled.
//
func SQS_longpolling_receive_message(name string, timeout int64, previousBodyMD5 *string) *string {

	// Initialize a session that the SDK will use to load configuration,
	// credentials, and region from the shared config file. (~/.aws/config).
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create a SQS service client.
	svc := sqs.New(sess)

	// Need to convert the queue name into a URL. Make the GetQueueUrl
	// API call to retrieve the URL. This is needed for receiving messages
	// from the queue.
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			exitErrorf("Unable to find queue %q.", name)
		}
		exitErrorf("Unable to queue %q, %v.", name, err)
	}

	// Receive a message from the SQS queue with long polling enabled.
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: resultURL.QueueUrl,
		AttributeNames: aws.StringSlice([]string{
			"SentTimestamp",
		}),
		MaxNumberOfMessages: aws.Int64(1),
		MessageAttributeNames: aws.StringSlice([]string{
			"All",
		}),
		WaitTimeSeconds: aws.Int64(timeout),
	})
	if err != nil {
		exitErrorf("Unable to receive message from queue %q, %v.", name, err)
	}

	log.Debugf("Received %d messages.", len(result.Messages))
	if len(result.Messages) > 0 {
		log.Debug(result.Messages)
	}

	if len(result.Messages) == 0 {
		return_value := ""
		return &return_value
	}

	// Have we already received this message? If so, leave it in the queue and carry on.
	if *result.Messages[0].MD5OfBody == *previousBodyMD5 {
		log.Info("Message contains current chaos. Not deleting: ", *result.Messages[0].Body)
		time.Sleep(time.Duration(1) * time.Minute)
		return_value := ""
		return &return_value
	}

	_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      resultURL.QueueUrl,
		ReceiptHandle: result.Messages[0].ReceiptHandle,
	})

	if err != nil {
		exitErrorf("Delete Error", err)
	}

	log.Debug("Message Deleted")

	// Set the message body MD% for comparison next time.
	*previousBodyMD5 = *result.Messages[0].MD5OfBody

	// Repost message for the other nodes.
	share_message(resultURL.QueueUrl, result.Messages[0], svc)

	return result.Messages[0].Body
}

// Repost message for other nodes with decremented counter.
func share_message(queueUrl *string, message *sqs.Message, svc *sqs.SQS) {
	/* Find Nodes counter within message attributes, and decrement the node counter. Then post to SQS queue for the other nodes.
	Ex)
	{"Command": "pause", "Interval": "1m", "Duration": "5s", "Nodes": 2 } +> {"Command": "pause", "Interval": "1m", "Duration": "5s", "Nodes": 1}
	*/

	if _, ok := message.MessageAttributes["Nodes"]; !ok {
		log.Debug("Node attribute not found. Not resending message for other nodes.")
		return
	}

	nodesCount, err := strconv.Atoi(*message.MessageAttributes["Nodes"].StringValue)

	if err != nil {
		log.Errorf("Nodes value: %s not an integer. %s", *message.MessageAttributes["Nodes"].StringValue, err)
		return
	}
	log.Debugf("Node number: %d", nodesCount)

	nodesCount--
	if nodesCount == 0 {
		log.Debug("Node Counter is now zero, not resending message")
		return
	}

	*message.MessageAttributes["Nodes"].StringValue = strconv.Itoa(nodesCount)

	log.Debug("Message after node decrement:\n", message)

	// Resend message with decremented Node count.
	_, err = svc.SendMessage(&sqs.SendMessageInput{
		MessageAttributes: message.MessageAttributes,
		MessageBody:       message.Body,
		QueueUrl:          queueUrl,
	})
	if err != nil {
		log.Error("Failed to resend message with decremented counter")
	}

	return
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
