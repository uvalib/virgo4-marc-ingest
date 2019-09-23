package main

import (
   "fmt"
   "io/ioutil"
   "log"
   "time"

   "github.com/aws/aws-sdk-go/aws"
   "github.com/aws/aws-sdk-go/aws/session"
   "github.com/aws/aws-sdk-go/service/s3"
   "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// taken from https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_download_object.go

func s3download( downloadDir string, bucket string, object string ) ( string, error ) {


   file, err := ioutil.TempFile( downloadDir, "" )
   if err != nil {
      return "", err
   }
   defer file.Close()

   start := time.Now()
   sourcename := fmt.Sprintf( "s3:/%s/%s", bucket, object )
   log.Printf("Downloading %s to %s", sourcename, file.Name( ) )

   sess, err := session.NewSession( )
   if err != nil {
      return "", err
   }

   downloader := s3manager.NewDownloader( sess )

   _, err = downloader.Download( file,
      &s3.GetObjectInput{
         Bucket: aws.String(bucket),
         Key:    aws.String(object),
      })

   if err != nil {
      return "", err
   }

   duration := time.Since(start)
   log.Printf("Download of %s complete in %0.2f seconds", sourcename, duration.Seconds() )
   return file.Name( ), nil
}

//
// end of file
//
