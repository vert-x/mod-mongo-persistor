# MongoDB Persistor

This module allows data to be saved, retrieved, searched for, and deleted in a MongoDB instance. MongoDB is a great match for persisting vert.x data since it natively handles JSON (BSON) documents. To use this module you must have a MongoDB instance running on your network.

This is a worker module and must be started as a worker verticle.

## Dependencies

This module requires a MongoDB server to be available on the network.

## Name

The module name is `mongo-persistor`.

## Configuration

The mongo-persistor module takes the following configuration:

    {
        "address": <address>,
        "host": <host>,
        "port": <port>,
        "db_name": <db_name>    
    }
    
For example:

    {
        "address": "test.my_persistor",
        "host": "192.168.1.100",
        "port": 27000
        "db_name": "my_db"
    }        
    
Let's take a look at each field in turn:

* `address` The main address for the module. Every module has a main address. Defaults to `vertx.mongopersistor`.
* `host` Host name or ip address of the MongoDB instance. Defaults to `localhost`.
* `port` Port at which the MongoDB instance is listening. Defaults to `27017`.
* `db_name` Name of the database in the MongoDB instance to use. Defaults to `default_db`.

## Operations

The module supports the following operations

### Save

Saves a document in the database.

To save a document send a JSON message to the module main address:

    {
        "action": "save",
        "collection": <collection>,
        "document": <document>
    }     
    
Where:
* `collection` is the name of the MongoDB collection that you wish to save the document in. This field is mandatory.
* `document` is the JSON document that you wish to save.

An example would be:

    {
        "action": "save",
        "collection": "users",
        "document": {
            "name": "tim",
            "age": 1000,
            "shoesize": 3.14159,
            "username": "tim",
            "password": "wibble"
        }
    }  
    
When the save complete successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok"
    }
    
The reply will also contain a field `_id` if the document that was saved didn't specifiy an id, this will be an automatically generated UUID, for example:

    {
        "status": "ok"
        "_id": "ffeef2a7-5658-4905-a37c-cfb19f70471d"
    }
     
If you save a document which already possesses an `_id` field, and a document with the same id already exists in the database, then the document will be updated. 
 
If an error occurs in saving the document a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where `message` is an error message.    

   
### Update

Updates a document in the database.
Uses the Mongodb update function: http://www.mongodb.org/display/DOCS/Updating

To update a document send a JSON message to the module main address:

    {
        "action": "update",
        "collection": <collection>,
        "criteria": {
            <criteria>
        },
        "objNew" : {
            <objNew>
        },
        upsert : <upsert>
        multi: <multi>
    }  

Where:
 * `collection` is the name of the MongoDB collection that you wish to save the document in. This field is mandatory.


An example would be:

    {
        "action": "update",
        "collection": "users",
        "criteria": {
            "_id": "tim",
        },
        objNew : {
            $inc: {
                age: 30
            }
         },
        upsert : true,
        multi : false
    }






### Find

Finds matching documents in the database.

To find documents send a JSON message to the module main address:

    {
        "action": "find",
        "collection": <collection>,
        "matcher": <matcher>,
        "keys": <keys>,
        "skip": <offset>,
        "limit": <limit>,
        "batch_size": <batch_size>
    }     
    
Where:
* `collection` is the name of the MongoDB collection that you wish to search in in. This field is mandatory.
* `matcher` is a JSON object that you want to match against to find matching documents. This obeys the normal MongoDB matching rues.
* `keys` is an optional JSON object that contains the fields that should be returned for matched documents. See MongoDB manual for more information. Example: { "name": 1 } will only return objects with _id and the name field
* `skip` is a number which determines the number of documents to skip. This is optional. By default no documents are skipped.
* `limit` is a number which determines the maximum total number of documents to return. This is optional. By default all documents are returned.
* `batch_size` is a number which determines how many documents to return in each reply JSON message. It's optional and the default value is `100`. Batching is discussed in more detail below.

An example would be:

    {
        "action": "find",
        "collection": "orders",
        "matcher": {
            "item": "cheese"
        }
    }  
    
This would return all orders where the `item` field has the value `cheese`. 

When the find complete successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok",
        "results": <results>
    }   
    
Where `results` is a JSON array containing the results of the find operation. For example:

    {
        "status": "ok",
        "results": [
            {
                "user": "tim",
                "item": "cheese",
                "total": 123.45
            },
            {
                "user": "bob",
                "item": "cheese",
                "total": 12.23
            },
            {
                "user": "jane",
                "item": "cheese",
                "total": 50.05
            }
        ]
    }
    
If an error occurs in finding the documents a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where `message` is an error message.

If you would like to paginate your result :

    {
        "action": "find",
        "collection": "orders",
        "skip"  : 10,
        "limit" : 10,
        "matcher": {}
    }

You decide to display 10 documents per page.
This message will retrieve second page.

Equivalence in mongoDB:

db.order.find().skip(10).limit(10)

### Count

Count all matching document in the database.

To count a document send a JSON message to the module main address:

    {
        "action": "count",
        "collection": <collection>,
        "matcher": <matcher>
    }

When the count complete successfully, a reply message is sent back to the sender with the following data:

    {
         "status": "ok",
         "count" : <number>
    }


#### Batching

If a find returns many documents we do not want to load them all up into memory at once and send them in a single JSON message since this could result in the server running out of RAM.

Instead, if there are more than `batch_size` documents to be found, the module will send a maxium of `batch_size` documents in each reply, and send multiple replies.

When you receive a reply to the find message containing `batch_size` documents the `status` field of the reply will be set to `more-exist` if there are more documents available.

To get the next batch of documents you just reply to the reply with an empty JSON message, and specify a reply handler in which to receive the next batch.

For instance, in JavaScript you might do something like:

    function processResults(results) {
        // Process the data
    }

    function createReplyHandler() {
        return function(reply, replier) {
            // Got some results - process them
            processResults(reply.results);
            if (reply.status === 'more-exist') {
                // Get next batch
                replier({}, createReplyHandler());
            }
        }
    }

    // Send the find request
    eb.send('foo.myPersistor', {
        action: 'find',
        collection: 'items',
        matcher: {}        
    }, createReplyHandler());
    
If there is more data to be requested and you do not reply to get the next batch within a timeout (10 seconds), then the underlying MongoDB cursor will be closed, and any further attempts to request more will fail.    
    

### Find One

Finds a single matching document in the database.

To find a document send a JSON message to the module main address:

    {
        "action": "findone",
        "collection": <collection>,
        "matcher": <matcher>,
        "keys": <keys>
    }     
    
Where:
* `collection` is the name of the MongoDB collection that you wish to search in in. This field is mandatory.
* `matcher` is a JSON object that you want to match against to find a matching document. This obeys the normal MongoDB matching rues.
* `keys` is an optional JSON object that contains the fields that should be returned for matched documents. See MongoDB manual for more information. Example: { "name": 1 } will only return objects with _id and the name field

If more than one document matches, just the first one will be returned.

An example would be:

    {
        "action": "findone",
        "collection": "items",
        "matcher": {
            "_id": "ffeef2a7-5658-4905-a37c-cfb19f70471d"
        }
    }  
    
This would return the item with the specified id.

When the find complete successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok",
        "result": <result>
    }       
    
If an error occurs in finding the documents a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where `message` is an error message. 

### Delete

Deletes a matching documents in the database.

To delete documents send a JSON message to the module main address:

    {
        "action": "delete",
        "collection": <collection>,
        "matcher": <matcher>
    }     
    
Where:
* `collection` is the name of the MongoDB collection that you wish to delete from. This field is mandatory.
* `matcher` is a JSON object that you want to match against to delete matching documents. This obeys the normal MongoDB matching rues.

All documents that match will be deleted.

An example would be:

    {
        "action": "delete",
        "collection": "items",
        "matcher": {
            "_id": "ffeef2a7-5658-4905-a37c-cfb19f70471d"
        }
    }  
    
This would delete the item with the specified id.

When the find complete successfully, a reply message is sent back to the sender with the following data:

    {
        "status": "ok",
        "number": <number>
    }       
    
Where `number` is the number of documents deleted.    
    
If an error occurs in finding the documents a reply is returned:

    {
        "status": "error",
        "message": <message>
    }
    
Where `message` is an error message.

### writeConcern
save, update and delete operations have an optional field called "writeConcern". Setting this property in your request
change the "consistency" or that operation.

This allows each call/use case to overwrite the db's default WriteConcern setting.

Since certain use cases might need more or less consistency than the databases default setting.

By default MongoDB sets the database to the least restrictive async call and forget WriteConcern, which can lead to data loss.

By being able to set it to a higher setting you can get to your eventual consistency.

The property is "writeConcern" and can be set to any of the constant names in the Java MongoDB Driver WriteConcern class as a String.

WriteConcern has a valueOf method that takes that String and converts it to a fully configured WriteConcern class.

Valid values are
"NONE", "NORMAL", "SAFE", "MAJORITY", "FSYNC_SAFE", "JOURNAL_SAFE" and "REPLICAS_SAFE"

An example for delete would be

    {
        "action": "delete",
        "collection": <collection>,
        "matcher": <matcher>,
        "writeConcert": "SAFE"
    }