// Package aws implements InputReader(s) that access AWS services (S3 and SQS)
package awsreaders

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gobs/logger"
)

var llog = logger.GetLogger(logger.INFO, "AWSREADERS")
var debug = false

// QueueReaderContext is the context for QueueReader
//
// It keeps track of the last message fetched from the queue in order to release it.
// If the client doesn't want the last message to be release it, it should set QueueReaaderContext.Last = nil
// before fetching the next message from the queue
type QueueReaderContext struct {
	Queue  string
	Client *sqs.Client
	Last   *sqs.Message
}

// QueueReader returns a list of messages read from the input SQS queue.
//
// Note that it always releases the old message when asking for a new one. If the client wants control
// on the old message it should use QueueReaderWithContext instead.
func QueueReader(queue string) chan string {
	awscfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		llog.Warning("cannot get AWS configuration: %v", err)
		return nil
	}

	client := sqs.New(awscfg)
	return QueueReaderWithContext(&QueueReaderContext{Queue: queue, Client: client})
}

// QueueReaderWithContext returns a list of messages read from the input SQS queue.
//
// It will normally release the last message before fetching a new one. If the client wants to "keep"
// the last message (so that it gets re-delivered) it should set ctx.Last = nil before fetching a new message.
func QueueReaderWithContext(ctx *QueueReaderContext) chan string {
	resp, err := ctx.Client.GetQueueUrlRequest(&sqs.GetQueueUrlInput{
		QueueName: aws.String(ctx.Queue),
	}).Send(context.TODO())
	if err != nil {
		llog.Warning("Invalid queue name %q: %v", ctx.Queue, err)

		if debug {
			resp, err := ctx.Client.ListQueuesRequest(&sqs.ListQueuesInput{}).Send(context.TODO())
			if err != nil {
				llog.Warning("Error listing queues")
			} else {
				llog.Debug("Available queues:")
				for _, q := range resp.QueueUrls {
					llog.Debug("  %v", q)
				}
			}
		}

		return nil
	}

	queueUrl := resp.QueueUrl

	ch := make(chan string)

	go func() {
		for {
			/*
			 * This is deleting the previous message before fetching a new one
			 * (assuming we are done with it).
			 *
			 * There should be a way to say "we don't want to delete it because
			 * something failed and we want to retry".
			 */
			if ctx.Last != nil {
				if _, err := ctx.Client.DeleteMessageRequest(&sqs.DeleteMessageInput{
					QueueUrl:      queueUrl,
					ReceiptHandle: ctx.Last.ReceiptHandle,
				}).Send(context.TODO()); err != nil {
					llog.Warning("Delete message: %v", err)
				}

				ctx.Last = nil
			}

			resp, err := ctx.Client.ReceiveMessageRequest(&sqs.ReceiveMessageInput{
				QueueUrl:        queueUrl,
				WaitTimeSeconds: aws.Int64(20),
			}).Send(context.TODO())
			if err != nil {
				llog.Warning("Receive %v: %v", ctx.Queue, err)
				time.Sleep(20 * time.Second)
			} else if len(resp.Messages) > 0 {
				llog.Debug("Receive from %q, got %v", ctx.Queue, len(resp.Messages))
				m := resp.Messages[0]
				ctx.Last = &m
				ch <- aws.StringValue(m.Body)
			} else {
				llog.Debug("Waiting on %q", ctx.Queue)
			}
		}

		close(ch)
	}()

	return ch
}

// BucketReader returns a "list" of object names as read by listing the input S3 bucket.
// if "short" is false each line contains the object info (key lastModified size)
func BucketReader(bucket, prefix, delim, start string, max int, short bool) chan string {
	awscfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		llog.Warning("cannot get AWS configuration: %v", err)
		return nil
	}

	client := s3.New(awscfg)
	ch := make(chan string)

	go func() {
		req := client.ListObjectsV2Request(&s3.ListObjectsV2Input{
			Bucket:     aws.String(bucket), // Required
			Delimiter:  aws.String(delim),
			MaxKeys:    aws.Int64(1),
			Prefix:     aws.String(prefix),
			StartAfter: aws.String(start),
		})

		p := s3.NewListObjectsV2Paginator(req)
		pageno := 0

		for p.Next(context.TODO()) {
			page := p.CurrentPage()
			pageno += 1

			for _, v := range page.Contents {
				if short {
					ch <- aws.StringValue(v.Key)
				} else {
					ch <- strings.Join([]string{
						aws.StringValue(v.Key),
						aws.TimeValue(v.LastModified).String(),
						strconv.FormatInt(aws.Int64Value(v.Size), 10)},
						" ")
				}
			}
		}

		if err := p.Err(); err != nil {
			llog.Warning("error on page %v: %v", pageno, err)
		}

		close(ch)
	}()

	return ch
}

// Enable additional debug logging
func Debug(d bool) {
	debug = d
	if debug {
		llog.SetLevel(logger.DEBUG)
	}
}
