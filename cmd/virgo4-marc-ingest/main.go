package main

import (
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
	marcRecordsChan := make( chan []byte, cfg.WorkerQueueSize )

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
			err = marcLoader(f, marcRecordsChan)
			if err != nil {
				log.Fatal(err)
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