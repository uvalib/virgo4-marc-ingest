package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	OutQueueName  string
	FileName      string
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")
	var cfg ServiceConfig
	flag.StringVar(&cfg.OutQueueName, "outqueue", "", "Outbound queue name")
	flag.StringVar(&cfg.FileName, "infile", "", "Batch file")

	flag.Parse()

	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName )
	log.Printf("[CONFIG] FileName             = [%s]", cfg.FileName )

	return &cfg
}
