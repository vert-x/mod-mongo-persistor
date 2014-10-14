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

import javax.net.ssl.SSLSocketFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * MongoDB Persistor Bus Module<p>
 * Please see the README.md for a full description<p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author Thomas Risberg
 * @author Richard Warburton
 */
public class MongoPersistor extends BusModBase implements Handler<Message<JsonObject>> {

  protected String address;
  protected String host;
  protected int port;
  protected String dbName;
  protected String username;
  protected String password;
  protected ReadPreference readPreference;
  protected boolean autoConnectRetry;
  protected int socketTimeout;
  protected boolean useSSL;

  protected Mongo mongo;
  protected DB db;

  @Override
  public void start() {
    super.start();

    address = getOptionalStringConfig("address", "vertx.mongopersistor");

    host = getOptionalStringConfig("host", "localhost");
    port = getOptionalIntConfig("port", 27017);
    dbName = getOptionalStringConfig("db_name", "default_db");
    username = getOptionalStringConfig("username", null);
    password = getOptionalStringConfig("password", null);
    readPreference = ReadPreference.valueOf(getOptionalStringConfig("read_preference", "primary"));
    int poolSize = getOptionalIntConfig("pool_size", 10);
    autoConnectRetry = getOptionalBooleanConfig("auto_connect_retry", true);
    socketTimeout = getOptionalIntConfig("socket_timeout", 60000);
    useSSL = getOptionalBooleanConfig("use_ssl", false);

    JsonArray seedsProperty = config.getArray("seeds");

    try {
      MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
      builder.connectionsPerHost(poolSize);
      builder.autoConnectRetry(autoConnectRetry);
      builder.socketTimeout(socketTimeout);
      builder.readPreference(readPreference);

      if (useSSL) {
        builder.socketFactory(SSLSocketFactory.getDefault());
      }

      if (seedsProperty == null) {
        ServerAddress address = new ServerAddress(host, port);
        mongo = new MongoClient(address, builder.build());
      } else {
        List<ServerAddress> seeds = makeSeeds(seedsProperty);
        mongo = new MongoClient(seeds, builder.build());
      }

      db = mongo.getDB(dbName);
      if (username != null && password != null) {
        db.authenticate(username, password.toCharArray());
      }
    } catch (UnknownHostException e) {
      logger.error("Failed to connect to mongo server", e);
    }
    eb.registerHandler(address, this);
  }

  private List<ServerAddress> makeSeeds(JsonArray seedsProperty) throws UnknownHostException {
    List<ServerAddress> seeds = new ArrayList<>();
    for (Object elem : seedsProperty) {
      JsonObject address = (JsonObject) elem;
      String host = address.getString("host");
      int port = address.getInteger("port");
      seeds.add(new ServerAddress(host, port));
    }
    return seeds;
  }

  @Override
  public void stop() {
    if (mongo != null) {
      mongo.close();
    }
  }

  @Override
  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");

    if (action == null) {
      sendError(message, "action must be specified");
      return;
    }

    try {

      // Note actions should not be in camel case, but should use underscores
      // I have kept the version with camel case so as not to break compatibility

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
        // no need for a backwards compatible "findAndModify" since this feature was added after 
        case "find_and_modify":
          doFindAndModify(message);
          break;
        case "delete":
          doDelete(message);
          break;
        case "count":
          doCount(message);
          break;
        case "getCollections":
        case "get_collections":
          getCollections(message);
          break;
        case "dropCollection":
        case "drop_collection":
          dropCollection(message);
          break;
        case "collectionStats":
        case "collection_stats":
          getCollectionStats(message);
          break;
        case "command":
          runCommand(message);
          break;
        default:
          sendError(message, "Invalid action: " + action);
      }
    } catch (MongoException e) {
      sendError(message, e.getMessage(), e);
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
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
    // Backwards compatibility
    if (writeConcern == null) {
      writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
    }
    if (writeConcern == null) {
      writeConcern = db.getWriteConcern();
    }
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

    JsonObject objNewJson = getMandatoryObject("objNew", message);
    if (objNewJson == null) {
      return;
    }
    DBObject objNew = jsonToDBObject(objNewJson);
    Boolean upsert = message.body().getBoolean("upsert", false);
    Boolean multi = message.body().getBoolean("multi", false);
    DBCollection coll = db.getCollection(collection);
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
    // Backwards compatibility
    if (writeConcern == null) {
      writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
    }

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
    Integer limit = (Integer) message.body().getNumber("limit");
    if (limit == null) {
      limit = -1;
    }
    Integer skip = (Integer) message.body().getNumber("skip");
    if (skip == null) {
      skip = -1;
    }
    Integer batchSize = (Integer) message.body().getNumber("batch_size");
    if (batchSize == null) {
      batchSize = 100;
    }
    Integer timeout = (Integer) message.body().getNumber("timeout");
    if (timeout == null || timeout < 0) {
      timeout = 10000; // 10 seconds
    }
    JsonObject matcher = message.body().getObject("matcher");
    JsonObject keys = message.body().getObject("keys");

    Object hint = message.body().getField("hint");
    Object sort = message.body().getField("sort");
    DBCollection coll = db.getCollection(collection);
    DBCursor cursor;
    if (matcher != null) {
      cursor = (keys == null) ?
          coll.find(jsonToDBObject(matcher)) :
          coll.find(jsonToDBObject(matcher), jsonToDBObject(keys));
    } else {
      cursor = coll.find();
    }
    if (skip != -1) {
      cursor.skip(skip);
    }
    if (limit != -1) {
      cursor.limit(limit);
    }
    if (sort != null) {
      cursor.sort(sortObjectToDBObject(sort));
    }
    if (hint != null) {
      if (hint instanceof JsonObject) {
        cursor.hint(jsonToDBObject((JsonObject) hint));
      } else if (hint instanceof String) {
        cursor.hint((String) hint);
      } else {
        throw new IllegalArgumentException("Cannot handle type " + hint.getClass().getSimpleName());
      }
    }
    sendBatch(message, cursor, batchSize, timeout);
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

  private void sendBatch(Message<JsonObject> message, final DBCursor cursor, final int max, final int timeout) {
    int count = 0;
    JsonArray results = new JsonArray();
    while (cursor.hasNext() && count < max) {
      DBObject obj = cursor.next();
      JsonObject m = new JsonObject(obj.toMap());
      results.add(m);
      count++;
    }
    if (cursor.hasNext()) {
      JsonObject reply = createBatchMessage("more-exist", results);

      // If the user doesn't reply within timeout, close the cursor
      final long timerID = vertx.setTimer(timeout, new Handler<Long>() {
        @Override
        public void handle(Long timerID) {
          container.logger().warn("Closing DB cursor on timeout");
          try {
            cursor.close();
          } catch (Exception ignore) {
          }
        }
      });


      message.reply(reply, new Handler<Message<JsonObject>>() {
        @Override
        public void handle(Message<JsonObject> msg) {
          vertx.cancelTimer(timerID);
          // Get the next batch
          sendBatch(msg, cursor, max, timeout);
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
      JsonObject m = new JsonObject(res.toMap());
      reply.putObject("result", m);
    }
    sendOK(message, reply);
  }
  
  private void doFindAndModify(Message<JsonObject> message) {
    String collectionName = getMandatoryString("collection", message);
    if (collectionName == null) {
      return;
    }
    JsonObject msgBody = message.body();
    DBObject update = jsonToDBObjectNullSafe(msgBody.getObject("update"));
    DBObject query = jsonToDBObjectNullSafe(msgBody.getObject("matcher"));
    DBObject sort = jsonToDBObjectNullSafe(msgBody.getObject("sort"));
    DBObject fields = jsonToDBObjectNullSafe(msgBody.getObject("fields"));
    boolean remove = msgBody.getBoolean("remove", false);
    boolean returnNew = msgBody.getBoolean("new", false);
    boolean upsert = msgBody.getBoolean("upsert", false);

    DBCollection collection = db.getCollection(collectionName);
    DBObject result = collection.findAndModify(query, fields, sort, remove,
      update, returnNew, upsert);

    JsonObject reply = new JsonObject();
    if (result != null) {
      JsonObject resultJson = new JsonObject(result.toMap());
      reply.putObject("result", resultJson);
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
    WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
    // Backwards compatibility
    if (writeConcern == null) {
      writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
    }

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

  private void dropCollection(Message<JsonObject> message) {

    JsonObject reply = new JsonObject();
    String collection = getMandatoryString("collection", message);

    if (collection == null) {
      return;
    }

    DBCollection coll = db.getCollection(collection);

    try {
      coll.drop();
      sendOK(message, reply);
    } catch (MongoException mongoException) {
      sendError(message, "exception thrown when attempting to drop collection: " + collection + " \n" + mongoException.getMessage());
    }
  }

  private void getCollectionStats(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);

    if (collection == null) {
      return;
    }

    DBCollection coll = db.getCollection(collection);
    CommandResult stats = coll.getStats();

    JsonObject reply = new JsonObject();
    reply.putObject("stats", new JsonObject(stats.toMap()));
    sendOK(message, reply);

  }

  private void runCommand(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();

    String command = getMandatoryString("command", message);

    if (command == null) {
      return;
    }

    DBObject commandObject = (DBObject) JSON.parse(command);
    CommandResult result = db.command(commandObject);

    reply.putObject("result", new JsonObject(result.toMap()));
    sendOK(message, reply);
  }
  
  private static DBObject jsonToDBObject(JsonObject object) {
      return new BasicDBObject(object.toMap());
  }  
  
  private static DBObject jsonToDBObjectNullSafe(JsonObject object) {
    if (object != null) {
      return new BasicDBObject(object.toMap());
    } else {
      return null;
    }
  }

}

