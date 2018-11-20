//node script to copy/rename binary files and move them into a new folder within a remote s3 bucket. Can be easily modified to move between two remote buckets.
//To use the script must be installed on a running EC2 instance with permissions to access the buckets. Run 'npm install' after loading the script to get required dependancies.
//A report file is generated to detail the renamed files with the file size and new name
//Example input csv:
//"11086581","assets/DAM/Designs/Licensed Designs/STRW - Star Wars/Space Cowboy comp.psd"
//"9393405","assets/DAM/Designs/Licensed Designs/STRW - Star Wars/Comic Relief comp.psd"
//author: Ethan Steiner - esteiner@nuxeo.com

'use strict';

//dependencies
const async = require('async');
const fs = require('fs');
const parse = require('csv-parse');

//get reference to S3 client
const AWS = require('aws-sdk');
const s3 = new AWS.S3({
  apiVersion: '2006-03-01'
});

//for creating MD5 hash
const md5File = require('md5-file/promise');

//configuration object
var config = require('./config');

var tmpFile, thisTry = 1,
  maxRetries = 10;

//create file stream for migration report
var reportStream = fs.createWriteStream('migration-report-with-filestream.txt', {
  'flags': 'a'
});

//read CSV file line by line
var parser = parse({
  delimiter: ','
}, function (err, data) {
  async.eachSeries(data, function (line, callback) {
    var bucketMap = {};
    bucketMap.hash = line[0];
    bucketMap.path = line[1];
    download(config.srcBucket, bucketMap.path, thisTry)
      .then(function (file) {
        createFileObject(config.srcBucket, bucketMap.path)
          .then(function (file) {
            renameAndCopy(config.dstBucket, file, bucketMap.path)
              .then(function (res) {
                fs.unlink(`/tmp/${tmpFile}.tmp`, (err) => {
                  if (err) {
                    console.log('Error cleaning up file system...' + err);
                    reject(err)
                  }
                  callback();
                });
              })
          })
      })
      .catch(function (err) {
        console.log('Couldn\'t complete the chain', err);
        callback(err);
      });
  })
});

//get file from source bucket
function download(sourceBucket, filePath) {
  let thisTry = 0;
  return new Promise(function (resolve, reject) {
    if (thisTry >= maxRetries) {
      reject("SCRIPT FAILURE -- StreamContentLengthMismatch error on this file: " + filePath);
      return;
    }

    tmpFile = filePathSanitize(filePath);
    const file = fs.createWriteStream(`/tmp/${tmpFile}.tmp`, {
      flags: 'w+'
    });

    console.log("Fetching " + filePath);
    while (thisTry < maxRetries) {
      const stream = s3.getObject({
        Bucket: sourceBucket,
        Key: filePath
      }).createReadStream()

      stream.on('error', (err) => {
        console.log('Error retrieving file', err);
        if (err.message = "StreamContentLengthMismatch") {
          console.log("retrying - attempt #" + thisTry);
          thisTry++
        } else {
          reject(err);
        }
      });
      stream.on('end', () => {
        resolve(file);
        thisTry = maxRetries + 1;
      });
      stream.pipe(file);
    }
  });
}

function createFileObject(srcBucket, filePath) {
  return new Promise(function (resolve, reject) {
    console.log("Retrieving File Metadata");
    s3.headObject({
      Bucket: srcBucket,
      Key: filePath
    }, function (err, res) {
      //log & reject errors to continue processing
      if (err) {
        console.log('Error retrieving metadata...', err);
        reject(err);
        return;
      }
      //create hash for s3 object
      console.log("Hashing");
      md5File(`/tmp/${tmpFile}.tmp`).then(hash => {
        res.hash = hash;
        console.log(filePath + " = " + hash);
        resolve(res);
      })
    })
  })
}

function renameAndCopy(destinationBucket, object, originalPath) {
  return new Promise(function (resolve, reject) {
    let dstKey = 'binaries-to-be-migrated/' + object.hash;
    var body = fs.createReadStream(`/tmp/${tmpFile}.tmp`);
    var s3obj = new AWS.S3({
      params: {
        Bucket: destinationBucket,
        Key: dstKey,
        ContentType: object.ContentType
      }
    })

    //upload hashed object to s3
    s3obj.upload({
        Body: body
      })
      .on('httpUploadProgress', function (evt) {
        console.log(evt);
      })
      .send(function (err, res) {
        //log & reject errors to continue processing
        if (err) {
          console.log('Error on upload', err);
          reject(err);
          return;
        } else {
          reportStream.write(object.hash + ',' + originalPath + '\n');
          console.log('Updload the binary is a success', res);
          resolve(res);
        }
      })
  });
}

function filePathSanitize(filePath) {
  var modTmpFile, truncTmpFile;
  // remove quotes
  var temporaryFile = filePath.replace(/\/+/g, '_');
  // remove spaces if file name length is too long for file system
  if (temporaryFile.length > 240) {
    modTmpFile = temporaryFile.replace(/ /g, '');
  } else {
    return temporaryFile;
  }
  // truncate name if file name length is too long - even without spaces
  if (modTmpFile && modTmpFile.length > 240) {
    var length = 240;
    console.log("truncated filename");
    truncTmpFile = modTmpFile.substring(0, length);
    //protect against filepaths that end with /
    if (truncTmpFile.endsWith('/')) {
      return truncTmpFile.split('/').join();
    } else {
      return truncTmpFile;
    }
  } else {
    console.log("removed spaces from filename");
    return modTmpFile;
  }
}

//execute
var inputFile = config.s3CsvFile;
fs.createReadStream(inputFile).pipe(parser);
