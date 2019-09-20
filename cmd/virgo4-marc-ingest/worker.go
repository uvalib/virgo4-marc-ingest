package main

import (
   "encoding/base64"
   "github.com/uvalib/virgo4-sqs-sdk/awssqs"
   "log"
   "time"
)

func worker( id int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages <- chan []byte, ) {

   count := uint( 1 )
   block := make( []string, 0, awssqs.MAX_SQS_BLOCK_COUNT )
   var message []byte
   for {

      timeout := false

      // process a message or wait...
      select {
      case message = <- messages:
         break
      case <- time.After( 5 * time.Second ):
         timeout = true
         break
      }

      // did we timeout, if so, we need to flush any pending work
      if timeout == false {

         // we need to base64 encode these
         enc := base64.StdEncoding.EncodeToString( message )

         block = append(block, enc)

         // have we reached a block size limit
         if count % awssqs.MAX_SQS_BLOCK_COUNT == 0 {

            // send the block
            err := sendMessages(aws, queue, block)
            if err != nil {
               log.Fatal(err)
            }

            // reset the block
            block = block[:0]
         }
         count++

         if count % 1000 == 0 {
            log.Printf("Worker %d processed %d records", id, count)
         }
      } else {

         // we timed out waitinf for new messages, lets flush what we have
         if len( block ) != 0 {

            // send the block
            err := sendMessages(aws, queue, block)
            if err != nil {
               log.Fatal(err)
            }

            // reset the block
            block = block[:0]

            log.Printf("Worker %d processed %d records (flush)", id, count)
         }
      }
   }

   // should never get here
}

func sendMessages( aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []string ) error {

   count := len( messages )
   if count == 0 {
      return nil
   }
   batch := make( []awssqs.Message, 0, count )
   for _, m := range messages {
      batch = append( batch, constructMessage( m ) )
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

func constructMessage( message string ) awssqs.Message {

   attributes := make( []awssqs.Attribute, 0, 1 )
   //attributes = append( attributes, awssqs.Attribute{ "op", "add" } )
   //attributes = append( attributes, awssqs.Attribute{ "src", filename } )
   attributes = append( attributes, awssqs.Attribute{ "type", "base64/marc"} )
   return awssqs.Message{ Attribs: attributes, Payload: awssqs.Payload( message )}
}

//
// end of file
//
