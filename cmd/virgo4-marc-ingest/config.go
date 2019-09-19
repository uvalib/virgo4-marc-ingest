package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	OutQueueName  string
	FileName      string
	MaxCount      uint
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")
	var cfg ServiceConfig
	flag.StringVar(&cfg.OutQueueName, "outqueue", "", "Outbound queue name")
	flag.StringVar(&cfg.FileName, "infile", "", "Batch file")
	flag.UintVar(&cfg.MaxCount, "max", 0, "Maximum number of records to ingest (0 is all of them)")

	flag.Parse()

	if len( cfg.OutQueueName ) == 0 {
		log.Fatalf( "OutQueueName cannot be blank" )
	}

	if len( cfg.FileName ) == 0 {
		log.Fatalf( "FileName cannot be blank" )
	}

	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName )
	log.Printf("[CONFIG] FileName             = [%s]", cfg.FileName )
	log.Printf("[CONFIG] MaxCount             = [%d]", cfg.MaxCount )

	return &cfg
}
