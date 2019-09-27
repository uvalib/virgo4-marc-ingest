package main

import (
	"io"
	"log"
	"os"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[ 0 ], Version( ) )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle( cfg.InQueueName )
	if err != nil {
		log.Fatal( err )
	}

	outQueueHandle, err := aws.QueueHandle( cfg.OutQueueName )
	if err != nil {
		log.Fatal( err )
	}

	// create the record channel
	marcRecordsChan := make( chan MarcRecord, cfg.WorkerQueueSize )

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go worker( w, aws, outQueueHandle, marcRecordsChan )
	}

	for {

		// notification that there is a new ingest file to be processed
		inbound, err := getIngestFiles( *cfg, aws, inQueueHandle )
		if err != nil {
			log.Fatal( err )
		}

		// stream the contents to the record queue, the workers will handle it from there
		for _, f := range inbound {

			// create a new loader
			loader, err := NewMarcLoader( f.LocalName )
			if err != nil {
				log.Fatal( err )
			}

			// if the file appears to be valid, process it
			count := 0
			err = loader.Validate( )
			if err == nil {
				log.Printf("Processing %s (%s)", f.SourceName, f.LocalName )
				rec, err := loader.First( true )
				 if err == nil {
				 	for {
						count++
						marcRecordsChan <- rec

						rec, err = loader.Next( true )
						if err != nil {
							if err == io.EOF {
								// this is our mechanism to indicate that the file was processed OK
								err = nil
							}
							break
						}
					}
				 }
			} else {
				// file is invalid in some manner and we should not processes it
			}

			loader.Done( )
			if err == nil {
				log.Printf("Done processing %s (%s). %d records", f.SourceName, f.LocalName, count )
			}

			// assume we have handled it correctly for now, we might have individual bogus records
			err = removeIngestFile( f )
			if err != nil {
				log.Fatal( err )
			}
		}
	}
}

//
// end of file
//