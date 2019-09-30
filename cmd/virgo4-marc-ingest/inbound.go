package main

import (
   "encoding/json"
   "github.com/uvalib/virgo4-sqs-sdk/awssqs"
   "log"
   "os"
   "time"
)

type InboundFile struct {

   SourceName string
   LocalName  string
   Size       int64
}

func getIngestFiles( config ServiceConfig, aws awssqs.AWS_SQS, inQueueHandle awssqs.QueueHandle ) ( []InboundFile, error ) {

   for {

      messages, err := aws.BatchMessageGet( inQueueHandle, 1, time.Duration( config.PollTimeOut ) * time.Second )
      if err != nil {
         return nil, err
      }

      // did we get anything to process
      if len( messages ) == 1 {

         log.Printf("Received a new notification" )

         // assume the message is an S3 event containing a list of one or more new objecxts
         newS3objects, err := decodeS3Event( messages[ 0 ] )
         if err != nil {
            return nil, err
         }

         opStatus, err := aws.BatchMessageDelete( inQueueHandle, messages )
         if err != nil {
            return nil, err
         }

         // check the operation results
         for ix, op := range opStatus {
            if op == false {
               log.Printf( "ERROR: message %d failed to delete", ix )
            }
         }

         // we have some objects to download
         if len( newS3objects ) != 0 {
            localFiles := make( []InboundFile, 0 )
            for _, s3 := range newS3objects {
               localname, err := s3download( config.DownloadDir, s3.S3.Bucket.Name, s3.S3.Object.Key )
               if err != nil {
                  return nil, err
               }
               localFiles = append( localFiles,
                  InboundFile{ SourceName: s3.S3.Object.Key, LocalName: localname, Size: s3.S3.Object.Size } )
            }

            return localFiles, nil
         } else {
            log.Printf("Not an interesting notification, ignoring it" )
         }

      } else {
         log.Printf("No new notifications..." )
      }
   }
}

//
// turn a message received from the inbound queue into a list of zero or more new S3 objects
//
func decodeS3Event( message awssqs.Message ) ( []S3EventRecord, error ) {

   events := Events{}
   err := json.Unmarshal([]byte( message.Payload ), &events)
   if err != nil {
      log.Printf("ERROR: json unmarshal: %s", err )
      return nil, err
   }
   return events.Records, nil
}

func removeIngestFile( inbound InboundFile ) error {

   log.Printf("Removing %s (%s)", inbound.LocalName, inbound.SourceName )
   return os.Remove( inbound.LocalName )
}

//
// end of file
//