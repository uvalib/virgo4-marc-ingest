package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handle from the queue name
	outQueueHandle, err := aws.QueueHandle( cfg.OutQueueName )
	if err != nil {
		log.Fatal( err )
	}

	file, err := os.Open( cfg.FileName )
	if err != nil {
		log.Fatal( err )
	}
	defer file.Close( )

	block := make( []string, 0, awssqs.MAX_SQS_BLOCK_COUNT )

	count := uint( 0 )
	start := time.Now()

	for {
        // read the next record
		raw, err := marcRead( file )

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}

		// we need to base64 encode these
		enc := base64.StdEncoding.EncodeToString( raw )

		count ++
		block = append( block, enc )

		// have we reached a block size limit
		if count % awssqs.MAX_SQS_BLOCK_COUNT == awssqs.MAX_SQS_BLOCK_COUNT - 1 {

			err := sendMessages( cfg, aws, outQueueHandle, block)
			if err != nil {
				log.Fatal( err )
			}

			// reset the block
			block = block[:0]
		}

		if count % 100 == 0 {
			duration := time.Since(start)
			log.Printf("Processed %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
		}

		if cfg.MaxCount > 0 && count >= cfg.MaxCount  {
			break
		}
	}

	// any remaining records?
	if len(block) != 0 {

		err := sendMessages( cfg, aws, outQueueHandle, block)
		if err != nil {
			log.Fatal( err )
		}
	}

	duration := time.Since(start)
	log.Printf("Done, processed %d records in %0.2f seconds (%0.2f tps)", count, duration.Seconds(), float64( count ) / duration.Seconds() )
}

func marcRead( infile io.Reader ) ( []byte, error ) {

	// read the 5 byte length header
	length_buf := make( []byte, 5 )
	_, err := infile.Read( length_buf )
	if err != nil {
       return nil, err
	}

    length, err := strconv.Atoi( string( length_buf ) )
	if err != nil {
		return nil, err
	}
    
    //. ensure the number is sane
    if length <= 5 {
    	return nil, fmt.Errorf( "Record prefix invalid (%s)", string( length_buf ) )
	}

    length -= 5
    read_buf := make( []byte, length )
	_, err = infile.Read( read_buf )
	if err != nil {
		return nil, err
	}

	// verify the end of record marker
	if read_buf[ length - 2 ] != 0x1e || read_buf[ length - 1 ] != 0x1d {
		return nil, fmt.Errorf( "Record suffix invalid (%x %x)", read_buf[ length - 2 ], read_buf[ length - 1 ] )
	}

	return read_buf, nil
}

func sendMessages( cfg * ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []string) error {

	count := len( messages )
	if count == 0 {
		return nil
	}
	batch := make( []awssqs.Message, 0, count )
	for _, m := range messages {
		batch = append( batch, constructMessage( cfg.FileName, m ) )
	}

	opStatus, err := aws.BatchMessagePut( queue, batch )
	if err != nil {
		return err
	}

	// check the operation results
	for ix, op := range opStatus {
		if op == false {
			log.Printf( "WARNING: message %d failed to send to outbound queue", ix )
		}
	}

	return nil
}

func constructMessage( filename string, message string ) awssqs.Message {

	attributes := make( []awssqs.Attribute, 0, 3 )
	attributes = append( attributes, awssqs.Attribute{ "op", "add" } )
	attributes = append( attributes, awssqs.Attribute{ "src", filename } )
	attributes = append( attributes, awssqs.Attribute{ "type", "base64/marc"} )
	return awssqs.Message{ Attribs: attributes, Payload: awssqs.Payload( message )}
}

//
// end of file
//