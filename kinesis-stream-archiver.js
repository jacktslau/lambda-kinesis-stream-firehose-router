console.log('Loading Kinesis Event Stream Archiver');

var AWS = require('aws-sdk');
var firehose = new AWS.Firehose();

var deliveryStreamName = process.env.DELIVERY_STREAM_NAME || 'YOUR_KINESIS_FIREHOSE_DELIVERY_STREAM_NAME'; // Kinesis Firehose Stream Name for archive

exports.handler = function(event, context) {

  var json = JSON.stringify(event, null, 2);
	console.log('Received event:', json);
  
  var datas = event.Records.map(function(record) {
    var seqNum = record.kinesis.sequenceNumber;
    var eventSource = record.eventSource;
    var eventName = record.eventName;
    var region = record.awsRegion;
    var eventSourceARN = record.eventSourceARN;
    var payload = new Buffer(record.kinesis.data, 'base64').toString('ascii');
    var data = {
      eventSource: eventSource,
      eventSourceARN: eventSourceARN,
      eventName: eventName,
      awsRegion: region,
      sequenceNumber: seqNum,
      data: payload
    }
    return data;
  });

  console.log('Decoded payload', datas)

  var params = {
    DeliveryStreamName: deliveryStreamName,
    Record: { 
      Data: JSON.stringify(datas)
    }
  };

  firehose.putRecord(params, function(err, data) {
    if (err) {
      console.log(err, err.stack); // an error occurred
      return context.fail();
    } else {
      console.log("Firehose Put Record Success:", data);           // successful response
      return context.succeed("Successfully processed " + event.Records.length + " records to firehose " + deliveryStreamName);
    }
  });

};
