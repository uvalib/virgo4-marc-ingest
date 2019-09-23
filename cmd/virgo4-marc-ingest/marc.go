package main

import (
   "fmt"
   "io"
   "log"
   "os"
   "strconv"
   "time"
)

// the size of the MARC record header
var headerSize = 5

func marcLoader( filename string, marcRecords chan []byte ) error {

   file, err := os.Open( filename )
   if err != nil {
      return err
   }
   defer file.Close( )

   count := uint( 0 )
   start := time.Now()

   for {
      // read the next record
      raw, err := marcRead(file)

      if err != nil {
         // are we done
         if err == io.EOF {
            break
         } else {
            return err
         }
      }
      count++
      marcRecords <- raw

      if count % 1000 == 0 {
         duration := time.Since(start)
         log.Printf("Queued %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
      }
   }

   duration := time.Since(start)
   log.Printf("Completed %s: %d records (%0.2f tps)", filename, count, float64( count ) / duration.Seconds() )

   return nil
}

func marcRead( infile io.Reader ) ( []byte, error ) {

   // read the 5 byte length header
   length_buf := make( []byte, headerSize )
   _, err := infile.Read( length_buf )
   if err != nil {
      return nil, err
   }

   length, err := strconv.Atoi( string( length_buf ) )
   if err != nil {
      return nil, err
   }

   //. ensure the number is sane
   if length <= headerSize {
      return nil, fmt.Errorf( "Record prefix invalid (%s)", string( length_buf ) )
   }

   length -= headerSize
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

//
// end of file
//
