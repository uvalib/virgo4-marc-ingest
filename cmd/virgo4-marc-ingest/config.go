package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig  struct {
	InQueueName       string   // SQS queue name for inbound documents
	OutQueueName      string   // SQS queue name for outbound documents
	PollTimeOut       int64    // the SQS queue timeout (in seconds)

	DataSourceName    string   // the name to associate the data with. Each record has metadata showing this value
	MessageBucketName string   // the bucket to use for large messages
	DownloadDir       string   // the S3 file download directory (local)

	WorkerQueueSize   int      // the inbound message queue size to feed the workers
	Workers           int      // the number of worker processes
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt( env string ) int {

	number := ensureSetAndNonEmpty( env )
	n, err := strconv.Atoi( number )
	fatalIfError( err )
	return n
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_IN_QUEUE" )
	cfg.OutQueueName = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_OUT_QUEUE" )
	cfg.PollTimeOut = int64( envToInt( "VIRGO4_MARC_INGEST_QUEUE_POLL_TIMEOUT" ) )
	cfg.DataSourceName = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_DATA_SOURCE" )
	cfg.MessageBucketName = ensureSetAndNonEmpty( "VIRGO4_SQS_MESSAGE_BUCKET" )
	cfg.DownloadDir = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_DOWNLOAD_DIR" )
	cfg.WorkerQueueSize = envToInt( "VIRGO4_MARC_INGEST_WORK_QUEUE_SIZE" )
	cfg.Workers = envToInt( "VIRGO4_MARC_INGEST_WORKERS" )

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName )
	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName )
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut )
	log.Printf("[CONFIG] DataSourceName       = [%s]", cfg.DataSourceName )
	log.Printf("[CONFIG] MessageBucketName    = [%s]", cfg.MessageBucketName )
	log.Printf("[CONFIG] DownloadDir          = [%s]", cfg.DownloadDir )
	log.Printf("[CONFIG] WorkerQueueSize      = [%d]", cfg.WorkerQueueSize )
	log.Printf("[CONFIG] Workers              = [%d]", cfg.Workers )

	return &cfg
}
