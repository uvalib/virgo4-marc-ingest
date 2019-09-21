package main

import (
   "io/ioutil"
   "log"

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

   log.Printf("Downloading s3:/%s/%s to %s", bucket, object, file.Name( ) )

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

   log.Printf("Download complete" )
   return file.Name( ), nil
}

//
// end of file
//
