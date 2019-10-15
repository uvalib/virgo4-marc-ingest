package main

import (
	"encoding/base64"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, outQueue1 awssqs.QueueHandle, outQueue2 awssqs.QueueHandle, records <-chan MarcRecord) {

	count := uint(1)
	block := make([]MarcRecord, 0, awssqs.MAX_SQS_BLOCK_COUNT)
	var record MarcRecord
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-records:
			break
		case <-time.After(flushTimeout):
			timeout = true
			break
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			block = append(block, record)

			// have we reached a block size limit
			if count%awssqs.MAX_SQS_BLOCK_COUNT == 0 {

				// send the block
				err := sendOutboundMessages(config, aws, outQueue1, outQueue2, block)
				fatalIfError(err)

				// reset the block
				block = block[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("Worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(block) != 0 {

				// send the block
				err := sendOutboundMessages(config, aws, outQueue1, outQueue2, block)
				fatalIfError(err)

				// reset the block
				block = block[:0]

				log.Printf("Worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 1
		}
	}

	// should never get here
}

func sendOutboundMessages(config ServiceConfig, aws awssqs.AWS_SQS, outQueue1 awssqs.QueueHandle, outQueue2 awssqs.QueueHandle, records []MarcRecord) error {

	count := len(records)
	if count == 0 {
		return nil
	}
	batch := make([]awssqs.Message, 0, count)
	for _, m := range records {
		batch = append(batch, constructMessage(m, config.DataSourceName))
	}

	opStatus1, err := aws.BatchMessagePut(outQueue1, batch)
	if err != nil {
		if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err
		}
	}

	// if one or more message failed...
	if err == awssqs.OneOrMoreOperationsUnsuccessfulError {

		// check the operation results
		for ix, op := range opStatus1 {
			if op == false {
				log.Printf("WARNING: message %d failed to send to outQueue1", ix)
			}
		}
	}

	opStatus2, err := aws.BatchMessagePut(outQueue2, batch)
	if err != nil {
		if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err
		}
	}

	// if one or more message failed...
	if err == awssqs.OneOrMoreOperationsUnsuccessfulError {

		// check the operation results
		for ix, op := range opStatus2 {
			if op == false {
				log.Printf("WARNING: message %d failed to send to outQueue2", ix)
			}
		}
	}

	return nil
}

func constructMessage(record MarcRecord, source string) awssqs.Message {

	id, _ := record.Id()
	attributes := make([]awssqs.Attribute, 0, 3)
	attributes = append(attributes, awssqs.Attribute{Name: "id", Value: id})
	attributes = append(attributes, awssqs.Attribute{Name: "type", Value: "base64/marc"})
	attributes = append(attributes, awssqs.Attribute{Name: "source", Value: source})
	return awssqs.Message{Attribs: attributes, Payload: []byte(base64.StdEncoding.EncodeToString(record.Raw()))}
}

//
// end of file
//
