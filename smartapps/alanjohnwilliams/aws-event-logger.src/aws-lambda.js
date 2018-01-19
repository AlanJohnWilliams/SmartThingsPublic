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

function recursivePut(context,events,index,attempt,success,failure) {
    var event = events[index];    
    event.time = Date.parse(event.time);
    console.log("Loading:\n", JSON.stringify(event));

    // Put into DynamoDB!
    attempt +=1;
    dynamodb.putItem({
        "TableName": TABLE_NAME,
        "Item": event
    }, function(err, data) {
        if (err) {
            failure +=1;
            console.log("Dynamo Failure: " + JSON.stringify(data, null, "  "));
            console.log("Dynamo Failure: " + err);
        } else {
            success +=1;
        }
        index +=1;
        if(index<events.length) {
            recursivePut(context,events,index,attempt,success,failure);
        } else {
            if(failure > 0) {
                //% Incorporate call back into smartthings cloud
                console.log("Returning: Failed. "+attempt+" attempts, "+failure+" failed, "+success+" succeed.");
                context.fail("ERROR: Dynamo failed. "+attempt+" attempts, "+failure+" failed, "+success+" succeed.");
            } else {
                //% Incorporate call back into smartthings cloud
                console.log("Returning: Succeed. "+attempt+" attempts, "+failure+" failed, "+success+" succeed.");
                context.succeed({ success: true });
            }
        }
	});    
}

exports.handler = function(request, context) {
    //console.log("Request received:\n", JSON.stringify(request));
    //console.log("Context received:\n", JSON.stringify(context));
    //console.log("Events received:\n", JSON.stringify(request.body.events));

    // Event Information Provided by SmartThings
    var body = JSON.parse(request.body);
    if("events" in body) {
        var events = body.events;
        var index = 0;
        var attempt = 0;
        var success = 0;
        var failure = 0;
        if(events.length > 0) recursivePut(context,events,index,attempt,success,failure);
    }else{
        context.fail("ERROR: no events in request payload.");
    }
}
