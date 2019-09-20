package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName    string
	OutQueueName   string
	PollTimeOut    int64
	DownloadDir    string
	Workers        int
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
	if err != nil {

		os.Exit(1)
	}
	return n
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_IN_QUEUE" )
	cfg.OutQueueName = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_OUT_QUEUE" )
	cfg.PollTimeOut = int64( envToInt( "VIRGO4_MARC_INGEST_QUEUE_POLL_TIMEOUT" ) )
	cfg.DownloadDir = ensureSetAndNonEmpty( "VIRGO4_MARC_INGEST_DOWNLOAD_DIR" )
	cfg.Workers = envToInt( "VIRGO4_MARC_INGEST_WORKERS" )

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName )
	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName )
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut )
	log.Printf("[CONFIG] DownloadDir          = [%s]", cfg.DownloadDir )
	log.Printf("[CONFIG] Workers              = [%d]", cfg.Workers )

	return &cfg
}
