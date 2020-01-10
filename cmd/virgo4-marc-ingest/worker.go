package main

import (
	"encoding/base64"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, outQueue awssqs.QueueHandle, cacheQueue awssqs.QueueHandle, records <-chan Record) {

	count := uint(0)
	block := make([]Record, 0, awssqs.MAX_SQS_BLOCK_COUNT)
	var record Record
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-records:

		case <-time.After(flushTimeout):
			timeout = true
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			block = append(block, record)

			// have we reached a block size limit
			if count != 0 && count%awssqs.MAX_SQS_BLOCK_COUNT == awssqs.MAX_SQS_BLOCK_COUNT-1 {

				// send the block
				err := sendOutboundMessages(config, aws, outQueue, cacheQueue, block)
				fatalIfError(err)

				// reset the block
				block = block[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("INFO: worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(block) != 0 {

				// send the block
				err := sendOutboundMessages(config, aws, outQueue, cacheQueue, block)
				fatalIfError(err)

				// reset the block
				block = block[:0]

				log.Printf("INFO: worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 0
		}
	}

	// should never get here
}

func sendOutboundMessages(config ServiceConfig, aws awssqs.AWS_SQS, outQueue awssqs.QueueHandle, cacheQueue awssqs.QueueHandle, records []Record) error {

	count := len(records)
	if count == 0 {
		return nil
	}

	//
	// we use copies of the messages for each queue because we want to ensure that new S3 objects are created
	// if not, we have multiple messages that share an external S3 object
	//

	batch1 := make([]awssqs.Message, 0, count)
	batch2 := make([]awssqs.Message, 0, count)
	for _, m := range records {
		msg := constructMessage(m)
		batch1 = append(batch1, msg)
		batch2 = append(batch2, msg)
	}

	opStatus1, err1 := aws.BatchMessagePut(outQueue, batch1)
	if err1 != nil {
		// if an error we can handle, retry
		if err1 == awssqs.ErrOneOrMoreOperationsUnsuccessful {
			log.Printf("WARNING: one or more items failed to send to the work queue, retrying...")

			// retry the failed items and bail out if we cannot retry
			err1 = retrySend(aws, outQueue, opStatus1, batch1)
		}

		// bail out if an error and let someone else handle it
		if err1 != nil {
			return err1
		}
	}

	// if we are configured to send items to the cache
	if cacheQueue != "" {

		opStatus2, err2 := aws.BatchMessagePut(cacheQueue, batch2)
		if err2 != nil {
			// if an error we can handle, retry
			if err2 == awssqs.ErrOneOrMoreOperationsUnsuccessful {
				log.Printf("WARNING: one or more items failed to send to the cache queue, retrying...")
				// retry the failed items and bail out if we cannot retry
				err2 = retrySend(aws, cacheQueue, opStatus2, batch2)
			}

			// bail out if an error and let someone else handle it
			if err2 != nil {
				return err2
			}
		}
	}

	// if we get here, everything worked as expected
	return nil
}

func retrySend(aws awssqs.AWS_SQS, outQueue awssqs.QueueHandle, opStatus []awssqs.OpStatus, records []awssqs.Message) error {

	retryBatch := make([]awssqs.Message, 0)

	// build the retry batch
	for ix, op := range opStatus {
		if op == false {
			retryBatch = append(retryBatch, records[ix])
		}
	}

	// make sure there are items to retry
	sz := len(retryBatch)
	if sz == 0 {
		return nil
	}

	retryCount := 0
	for {
		retryCount += 1
		log.Printf("INFO: retrying %d item(s)", sz)

		_, err := aws.BatchMessagePut(outQueue, retryBatch)
		// success...
		if err == nil {
			return nil
		}

		// not success, anything other than an error we can retry, give up
		if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
			return err
		}

		// give up...
		if retryCount == 3 {
			log.Printf("ERROR: retry failed, giving up")
			return err
		}

		// sleep for a while
		time.Sleep(100 * time.Millisecond)
	}
}

func constructMessage(record Record) awssqs.Message {

	id, _ := record.Id()
	attributes := make([]awssqs.Attribute, 0, 4)
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordId, Value: id})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordType, Value: awssqs.AttributeValueRecordTypeB64Marc})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordSource, Value: record.Source()})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordOperation, Value: awssqs.AttributeValueRecordOperationUpdate})
	return awssqs.Message{Attribs: attributes, Payload: []byte(base64.StdEncoding.EncodeToString(record.Raw()))}
}

//
// end of file
//
