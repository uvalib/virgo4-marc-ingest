package main

import (
	"encoding/base64"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, outQueue1 awssqs.QueueHandle, outQueue2 awssqs.QueueHandle, records <-chan Record) {

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
				err := sendOutboundMessages(config, aws, outQueue1, outQueue2, block)
				if err != nil {
					if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
						fatalIfError(err)
					}
				}

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
				err := sendOutboundMessages(config, aws, outQueue1, outQueue2, block)
				if err != nil {
					if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
						fatalIfError(err)
					}
				}

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

func sendOutboundMessages(config ServiceConfig, aws awssqs.AWS_SQS, outQueue1 awssqs.QueueHandle, outQueue2 awssqs.QueueHandle, records []Record) error {

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
		msg := constructMessage(m, config.DataSourceName)
		batch1 = append(batch1, msg)
		batch2 = append(batch2, msg)
	}

	opStatus1, err1 := aws.BatchMessagePut(outQueue1, batch1)
	if err1 != nil {
		if err1 != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err1
		}
	}

	// if one or more message failed...
	if err1 == awssqs.OneOrMoreOperationsUnsuccessfulError {

		// check the operation results
		for ix, op := range opStatus1 {
			if op == false {
				log.Printf("WARNING: message %d failed to send to outQueue1", ix)
			}
		}
	}

	opStatus2, err2 := aws.BatchMessagePut(outQueue2, batch2)
	if err2 != nil {
		if err2 != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err2
		}
	}

	// if one or more message failed...
	if err2 == awssqs.OneOrMoreOperationsUnsuccessfulError {

		// check the operation results
		for ix, op := range opStatus2 {
			if op == false {
				log.Printf("WARNING: message %d failed to send to outQueue2", ix)
			}
		}
	}

	// report that some of the messages were not processed
	if err1 == awssqs.OneOrMoreOperationsUnsuccessfulError || err2 == awssqs.OneOrMoreOperationsUnsuccessfulError {
		return awssqs.OneOrMoreOperationsUnsuccessfulError
	}

	return nil
}

func constructMessage(record Record, source string) awssqs.Message {

	id, _ := record.Id()
	attributes := make([]awssqs.Attribute, 0, 4)
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordId, Value: id})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordType, Value: awssqs.AttributeValueRecordTypeB64Marc})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordSource, Value: source})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordOperation, Value: awssqs.AttributeValueRecordOperationUpdate})
	return awssqs.Message{Attribs: attributes, Payload: []byte(base64.StdEncoding.EncodeToString(record.Raw()))}
}

//
// end of file
//
