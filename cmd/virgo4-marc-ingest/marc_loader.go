package main

import (
   "fmt"
   "io"
   "log"
   "os"
   "strconv"
)

//var BadFileFormatError = fmt.Errorf( "Unrecognized file format" )
var BadMarcRecordError = fmt.Errorf( "Bad MARC record encountered" )
var BadRecordIdError = fmt.Errorf( "Bad MARC record identifier" )
var FileNotOpenError = fmt.Errorf( "File is not open" )

// the MarcLoader interface
type MarcLoader interface {

   Validate( ) error
   First( bool ) ( MarcRecord, error )
   Next( bool ) ( MarcRecord, error )
   Done( )
}

// the Marc record interface
type MarcRecord interface {

   Id( ) ( string, error )
   Raw( ) []byte
}

// this is our loader implementation
type marcLoaderImpl struct {
   File       * os.File
   HeaderBuff []byte
}

// this is our record implementation
type marcRecordImpl struct {
   RawBytes []byte
   marcId   string
}

// the size of the MARC record header
var marcRecordHeaderSize = 5
var marcRecordFieldDirStart = 24
var marcRecordFieldDirEntrySize = 12

// terminator sentinel values
var fieldTerminator = byte( 0x1e )
var recordTerminator = byte( 0x1d )

// and the factory
func NewMarcLoader( filename string ) ( MarcLoader, error ) {

   file, err := os.Open( filename )
   if err != nil {
      return nil, err
   }

   buf := make( []byte, marcRecordHeaderSize )
   return &marcLoaderImpl{ File: file, HeaderBuff: buf }, nil
}

// read all the records to ensure the file is valid
func ( l * marcLoaderImpl ) Validate( ) error {

   if l.File == nil {
      return FileNotOpenError
   }

   // get the first record and error out if bad
   _, err := l.First( false )
   if err != nil {
      return err
   }

   // read all the records and bail on the first failure except EOF
   for {
      _, err = l.Next( false )

      if err != nil {
         // are we done
         if err == io.EOF {
            break
         } else {
            return err
         }
      }
   }

   // everything is OK
   return nil
}

func ( l * marcLoaderImpl ) First( readAhead bool ) ( MarcRecord, error ) {

   if l.File == nil {
      return nil, FileNotOpenError
   }

   // go to the start of the file and then get the next record
   _, err := l.File.Seek( 0, 0 )
   if err != nil {
      return nil, err
   }

   return l.Next( readAhead )
}

func ( l * marcLoaderImpl ) Next( readAhead bool ) ( MarcRecord, error ) {

   if l.File == nil {
      return nil, FileNotOpenError
   }

   rec, err := l.rawMarcRead( )
   if err != nil {
      return nil, err
   }

   id, err := rec.Id( )
   if err != nil {
      return nil, err
   }

   //log.Printf( "INFO: marc record id: %s", id )

   //
   // there are times when the following record is really part of the current record. If this is the case, the 2 (or more)
   // records are given the same id. Attempt to handle this here.
   //
   if readAhead == true {

      for {
         // get the current position, assume no error cos we are not moving the file pointer
         currentPos, _ := l.File.Seek( 0, 1 )

         // get the next record
         nextRec, err := l.rawMarcRead()
         if err != nil {

            // if we error move the file pointer back and return the previously read record without error
            _, _ = l.File.Seek( currentPos, 0 )
            return rec, nil
         }

         nextId, err := nextRec.Id( )
         if err != nil {
            // if we error move the file pointer back and return the previously read record without error
            _, _ = l.File.Seek( currentPos, 0 )
            return rec, nil
         }

         if id != nextId {
            // if the id's do not match move the file pointer back and return the previously read record
            _, _ = l.File.Seek( currentPos, 0 )
            return rec, nil
         }

         // the id's match so we should append the contents of the next record onto the contents of the previous record
         // and repeat the process
         log.Printf( "WARNING: identified additional marc record for %s, appending it", id )
         copy( rec.Raw( ), nextRec.Raw( ) )
      }
   }

   return rec, nil
}

func ( l * marcLoaderImpl ) Done( ) {

   if l.File != nil {
      l.File.Close( )
      l.File = nil
   }
}

func ( l * marcLoaderImpl ) rawMarcRead( ) ( MarcRecord, error ) {

   // read the 5 byte length header
   _, err := l.File.Read( l.HeaderBuff )
   if err != nil {
      return nil, err
   }

   // is it potentially a record length?
   length, err := strconv.Atoi( string( l.HeaderBuff ) )
   if err != nil {
      return nil, err
   }

   // ensure the number is sane
   if length <= marcRecordHeaderSize {
      log.Printf( "ERROR: marc record prefix invalid (%s)", string( l.HeaderBuff ) )
      return nil, BadMarcRecordError
   }

   // we need to include the header in the raw record so move back so we read it again
   _, err = l.File.Seek( int64( -marcRecordHeaderSize ), 1 )
   if err != nil {
      return nil, err
   }

   readBuf := make( []byte, length )
   readBytes, err := l.File.Read(readBuf)
   if err != nil {
      return nil, err
   }

   // we did not read the number of bytes we expected, log it and declare victory
   if readBytes != length {
      log.Printf( "WARNING: short record read. Expected %d, got %d. Declaring EOF", length, readBytes )
      return nil, io.EOF
   }

   // verify the end of record marker exists and return success if it does
   if readBuf[ length - 2 ] == fieldTerminator && readBuf[ length - 1 ] == recordTerminator {
      return &marcRecordImpl{RawBytes: readBuf}, nil
   }

   // we will assume that the MARC header specified a larger value than necessary so we can track back to see if the
   // end of record marker comes earlier with the remaining bytes
   log.Printf( "WARNING: unexpected marc record suffix (%x %x)", readBuf[ length - 2 ], readBuf[ length - 1 ] )

   log.Printf( "FIXME: %s", string( readBuf ) )
   return nil, BadMarcRecordError
   //return &marcRecordImpl{ RawBytes: read_buf }, nil
}

func ( r * marcRecordImpl ) Id( ) ( string, error ) {

   if r.marcId != "" {
      return r.marcId, nil
   }

   return r.extractId( )
}

func ( r * marcRecordImpl ) Raw( ) []byte {
   return r.RawBytes
}

func ( r * marcRecordImpl ) extractId( ) ( string, error ) {

   id, err := r.getMarcFieldId( "001" )
   if err != nil {
      id, err = r.getMarcFieldId( "035" )
   }

   if err != nil {
      return "", err
   }

   // ensure the first character of the Id us a 'u' character
   if id[ 0 ] != 'u' {
      log.Printf( "ERROR: marc record id is suspect (%s)", id )
      return "", BadRecordIdError
   }

   r.marcId = id

   //fmt.Printf( "ID: %s\n", r.marcId )
   return r.marcId, nil
}

//
// A marc record consists of a 5 byte length header (in ascii) followed by a 'directory' of fields.
// The offset of the end of the directory is specified at byte 12 for 5 bytes.
// All field descriptors within the directory consist of the following:
//    field Id (string) bytes 0 - 2 (3 bytes)
//    field length (string) bytes 3 - 6 (4 bytes)
//    field offset (string) bytes 7 - 11 (5 bytes)
//
// The actual field values begin after the end of the directory.
//

func ( r * marcRecordImpl ) getMarcFieldId( fieldId string ) ( string, error ) {

   currentOffset := marcRecordFieldDirStart
   endOfDir, err := strconv.Atoi( string( r.RawBytes[12:17] ) )
   if err != nil {
      log.Printf( "ERROR: marc record end of directory offset invalid (%s)", string( r.RawBytes[12:17] ) )
      return "", BadMarcRecordError
   }

   // make sure we are actually pointing where we expect
   if endOfDir == 99999 || r.RawBytes[endOfDir - 1:endOfDir][0] != fieldTerminator {
      log.Printf( "BEEP BOOP special case..." )
   }

   for currentOffset < endOfDir {
      dirEntry := r.RawBytes[ currentOffset:currentOffset + marcRecordFieldDirEntrySize]
      //log.Printf( "Dir entry [%s]", string( dirEntry ) )
      fieldNumber := string( dirEntry[0:3] )
      next := string( dirEntry[3:7] )
      fieldLength, err := strconv.Atoi( next )
      if err != nil {
         log.Printf( "ERROR: marc record field length invalid (%s)", next )
         return "", BadMarcRecordError
      }
      next = string( dirEntry[7:12] )
      fieldOffset, err := strconv.Atoi( next )
      if err != nil {
         log.Printf( "ERROR: marc record field offset invalid (%s)", next )
         return "", BadMarcRecordError
      }

      //log.Printf( "Found field number %s. Offset %d, Length %d", fieldNumber, fieldOffset, fieldLength )
      if fieldNumber == fieldId {
         fieldStart := endOfDir + fieldOffset
         return string( r.RawBytes[ fieldStart:fieldStart + fieldLength - 1] ), nil
      }
      currentOffset += marcRecordFieldDirEntrySize
   }

   log.Printf( "ERROR: could not locate field %s in marc record", fieldId )
   return "", BadMarcRecordError
}

//
// end of file
//
