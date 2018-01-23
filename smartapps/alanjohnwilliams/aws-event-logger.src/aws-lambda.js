//
// Creates a DynamoDB row with your IoT data.
// By Nic Jansma http://nicj.net
//
// https://github.com/nicjansma/dht-logger/
//
// Based on: https://snowulf.com/2015/08/05/tutorial-aws-api-gateway-to-lambda-to-dynamodb/
//

//
// Imports
//
var doc = require("dynamodb-doc");

//
// Constants
//
var TABLE_NAME = "frisco";

var dynamodb = new doc.DynamoDB();

function recursivePut(events,s,callback) {

    var event = events[s.index];    
    event.time = Date.parse(event.time);
    for (var propName in event) { 
        if (event[propName] === null || event[propName] === undefined || event[propName] === "") {
            delete event[propName];
        }
    }
    console.log("Loading:\n", JSON.stringify(event));

    // Put into DynamoDB!
    s.attempted +=1;
    dynamodb.putItem({
        "TableName": TABLE_NAME,
        "Item": event
    }, function(err, data) {
        if (err) {
            s.failed +=1;
            console.log("Dynamo Failure: " + JSON.stringify(data, null, "  "));
            console.log("Dynamo Failure: " + err);
        } else {
            s.succeed +=1;
        }
        s.index +=1;
        console.log("Progress: "+s.attempted+" attempts, "+s.failed+" failed, "+s.succeed+" succeed.");
        if(s.index<events.length) {
            console.log("Recursive Call. Iteration "+s.attempted+" of "+s.eventcount);
            recursivePut(events,s,callback);
        } else {
            if(s.failed > 0) {
                var msg = "Failed. "+s.attempted+" attempts, "+s.failed+" failed, "+s.succeed+" succeed.";
                console.log(msg);
                callback(new Error(msg),response);
            } else {
                var msg = "Succeed. "+s.attempted+" attempts, "+s.failed+" failed, "+s.succeed+" succeed.";
                console.log(msg)
                var response = {
                    "statusCode": 200,
                    "headers": {
                    },
                    "body": JSON.stringify(s),
                    "isBase64Encoded": false
                };
                callback(null,response);
            }
        }
	});    
}

exports.handler = function(request, context, callback) {
    //console.log("Request received:\n", JSON.stringify(request));
    //console.log("Context received:\n", JSON.stringify(context));
    //console.log("Events received:\n", JSON.stringify(request.body.events));

    // Event Information Provided by SmartThings
    var body = JSON.parse(request.body);
    if("events" in body) {
        var events = body.events;
        var s = {
            index: 0,
            attempted: 0,
            succeed: 0,
            failed: 0,
            success: true,
            eventcount: events.length
        };
        if(s.eventcount > 0) {
            recursivePut(events,s,callback);
        }else{
            callback(new Error("Failure: 400: events field is empty"));
        }
    } else {
        callback(new Error("Failure: 400: No events field in payload"));
    }
}
