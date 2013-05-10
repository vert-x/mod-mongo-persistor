/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.vertx.mods;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.UnknownHostException;
import java.util.UUID;

/**
 * MongoDB Persistor Bus Module<p>
 * Please see the README.md for a full descrition<p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author Thomas Risberg
 */
public class MongoPersistor extends BusModBase implements Handler<Message<JsonObject>> {

  protected String address;
  protected String host;
  protected int port;
  protected String dbName;
  protected String username;
  protected String password;

  protected Mongo mongo;
  protected DB db;

  public void start() {
    super.start();

    address = getOptionalStringConfig("address", "vertx.mongopersistor");
    host = getOptionalStringConfig("host", "localhost");
    port = getOptionalIntConfig("port", 27017);
    dbName = getOptionalStringConfig("db_name", "default_db");
    username = getOptionalStringConfig("username", null);
    password = getOptionalStringConfig("password", null);
    int poolSize = getOptionalIntConfig("pool_size", 10);

    try {
      MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
      builder.connectionsPerHost(poolSize);
      ServerAddress address = new ServerAddress(host, port);
      mongo = new MongoClient(address, builder.build());
      db = mongo.getDB(dbName);
      if (username != null && password != null) {
        db.authenticate(username, password.toCharArray());
      }
    } catch (UnknownHostException e) {
      logger.error("Failed to connect to mongo server", e);
    }
    eb.registerHandler(address, this);
  }

  public void stop() {
    mongo.close();
  }

  public void handle(Message<JsonObject> message) {

    String action = message.body().getString("action");

    if (action == null) {
      sendError(message, "action must be specified");
      return;
    }

    switch (action) {
      case "save":
        doSave(message);
        break;
      case "update":
        doUpdate(message);
        break;    
      case "find":
        doFind(message);
        break;
      case "findone":
        doFindOne(message);
        break;
      case "delete":
        doDelete(message);
        break;
      case "count":
        doCount(message);
        break;
      case "getCollections":
        getCollections(message);
        break;
      case "collectionStats":
        getCollectionStats(message);
        break;
      case "command":
        runCommand(message);
        break;
      default:
        sendError(message, "Invalid action: " + action);
    }
  }

  private void doSave(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject doc = getMandatoryObject("document", message);
    if (doc == null) {
      return;
    }
    String genID;
    if (doc.getField("_id") == null) {
      genID = UUID.randomUUID().toString();
      doc.putString("_id", genID);
    } else {
      genID = null;
    }
    DBCollection coll = db.getCollection(collection);
    DBObject obj = jsonToDBObject(doc);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern",""));
    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    writeConcern = WriteConcern.SAFE;
    WriteResult res = coll.save(obj, writeConcern);
    if (res.getError() == null) {
      if (genID != null) {
        JsonObject reply = new JsonObject();
        reply.putString("_id", genID);
        sendOK(message, reply);
      } else {
        sendOK(message);
      }
    } else {
      sendError(message, res.getError());
    }
  }

  private void doUpdate(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject criteriaJson = getMandatoryObject("criteria", message);
    if (criteriaJson == null) {
      return;
    }
    DBObject criteria = jsonToDBObject(criteriaJson);

    JsonObject objNewJson =  getMandatoryObject("objNew", message);
    if (objNewJson == null) {
      return;
    }
    DBObject objNew = jsonToDBObject(objNewJson);
    Boolean upsert =  message.body().getBoolean("upsert",false);
    Boolean multi = message.body().getBoolean("multi",false);
    DBCollection coll = db.getCollection(collection);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern",""));
    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    WriteResult res = coll.update(criteria, objNew, upsert, multi, writeConcern);
    if (res.getError() == null) {
      JsonObject reply = new JsonObject();
      reply.putNumber("number", res.getN());
      sendOK(message, reply);
    } else {
      sendError(message, res.getError());
    }
  }

  private void doFind(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    Integer limit = (Integer)message.body().getNumber("limit");
    if (limit == null) {
      limit = -1;
    }
    Integer skip = (Integer)message.body().getNumber("skip");
    if (skip == null) {
      skip = -1;
    }
    Integer batchSize = (Integer)message.body().getNumber("batch_size");
    if (batchSize == null) {
      batchSize = 100;
    }
    JsonObject matcher = getMandatoryObject("matcher", message);
    if (matcher == null) {
      return;
    }
    JsonObject keys = message.body().getObject("keys");
    
    Object sort = message.body().getField("sort");
    DBCollection coll = db.getCollection(collection);
    DBCursor cursor = (keys == null) ? 
    			coll.find(jsonToDBObject(matcher)) : 
    			coll.find(jsonToDBObject(matcher), jsonToDBObject(keys));
    if (skip != -1) {
      cursor.skip(skip);
    }
    if (limit != -1) {
      cursor.limit(limit);
    }
    if (sort != null) {
      cursor.sort(sortObjectToDBObject(sort));
    }
    sendBatch(message, cursor, batchSize);
  }

  private DBObject sortObjectToDBObject(Object sortObj) {
    if (sortObj instanceof JsonObject) {
      // Backwards compatability and a simpler syntax for single-property sorting
      return jsonToDBObject((JsonObject) sortObj);
    } else if (sortObj instanceof JsonArray) {
      JsonArray sortJsonObjects = (JsonArray) sortObj;
      DBObject sortDBObject = new BasicDBObject();
      for (Object curSortObj : sortJsonObjects) {
        if (!(curSortObj instanceof JsonObject)) {
          throw new IllegalArgumentException("Cannot handle type "
              + curSortObj.getClass().getSimpleName());
        }

        sortDBObject.putAll(((JsonObject) curSortObj).toMap());
      }

      return sortDBObject;
    } else {
      throw new IllegalArgumentException("Cannot handle type " + sortObj.getClass().getSimpleName());
    }
  }

  private void sendBatch(Message<JsonObject> message, final DBCursor cursor, final int max) {
    int count = 0;
    JsonArray results = new JsonArray();
    while (cursor.hasNext() && count < max) {
      DBObject obj = cursor.next();
      String s = obj.toString();
      JsonObject m = new JsonObject(s);
      results.add(m);
      count++;
    }
    if (cursor.hasNext()) {
      JsonObject reply = createBatchMessage("more-exist", results);

      // Set a timeout, if the user doesn't reply within 10 secs, close the cursor
      final long timerID = vertx.setTimer(10000, new Handler<Long>() {
        public void handle(Long timerID) {
          container.logger().warn("Closing DB cursor on timeout");
          try {
            cursor.close();
          } catch (Exception ignore) {
          }
        }
      });


      message.reply(reply, new Handler<Message<JsonObject>>() {
        public void handle(Message<JsonObject> msg) {
          vertx.cancelTimer(timerID);
          // Get the next batch
          sendBatch(msg, cursor, max);
        }
      });

    } else {
      JsonObject reply = createBatchMessage("ok", results);
      message.reply(reply);
      cursor.close();
    }
  }

  private JsonObject createBatchMessage(String status, JsonArray results) {
    JsonObject reply = new JsonObject();
    reply.putArray("results", results);
    reply.putString("status", status);
    reply.putNumber("number", results.size());
    return reply;
  }

  protected void sendMoreExist(String status, Message<JsonObject> message, JsonObject json) {
    json.putString("status", status);
    message.reply(json, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> msg) {

      }
    });
  }

  private void doFindOne(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject matcher = message.body().getObject("matcher");
    JsonObject keys = message.body().getObject("keys");
    DBCollection coll = db.getCollection(collection);
    DBObject res;
    if (matcher == null) {
      res = keys != null ? coll.findOne(null, jsonToDBObject(keys)) : coll.findOne();
    } else {
      res = keys != null ? coll.findOne(jsonToDBObject(matcher), jsonToDBObject(keys)) : coll.findOne(jsonToDBObject(matcher));
    }
    JsonObject reply = new JsonObject();
    if (res != null) {
      String s = res.toString();
      JsonObject m = new JsonObject(s);
      reply.putObject("result", m);
    }
    sendOK(message, reply);
  }

    private void doCount(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            return;
        }
        JsonObject matcher = message.body().getObject("matcher");
        DBCollection coll = db.getCollection(collection);
        long count;
        if (matcher == null) {
            count = coll.count();
        } else {
            count = coll.count(jsonToDBObject(matcher));
        }
        JsonObject reply = new JsonObject();
        reply.putNumber("count", count);
        sendOK(message, reply);
    }

  private void doDelete(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return;
    }
    JsonObject matcher = getMandatoryObject("matcher", message);
    if (matcher == null) {
      return;
    }
    DBCollection coll = db.getCollection(collection);
    DBObject obj = jsonToDBObject(matcher);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern",""));
    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
    WriteResult res = coll.remove(obj, writeConcern);
    int deleted = res.getN();
    JsonObject reply = new JsonObject().putNumber("number", deleted);
    sendOK(message, reply);
  }

  private void getCollections(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();
    reply.putArray("collections", new JsonArray(db.getCollectionNames()
        .toArray()));
    sendOK(message, reply);
  }

  private void getCollectionStats(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);

    if (collection == null) {
      return;
    }
    
    DBCollection coll = db.getCollection(collection);
    CommandResult stats = coll.getStats();
    
    JsonObject reply = new JsonObject();
    reply.putObject("stats", new JsonObject(stats.toString()));
    sendOK(message, reply);

  }

  private void runCommand(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();
    
    String command = getMandatoryString("command", message);

    if (command == null) {
      return;
    }
    
    CommandResult result = db.command(command);
    
    reply.putObject("result", new JsonObject(result.toString()));
    sendOK(message, reply);
  }

  private DBObject jsonToDBObject(JsonObject object) {
    String str = object.encode();
    return (DBObject)JSON.parse(str);
  }

}

