package org.vertx.mods.mongo.test.integration.java;
/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.Binary;
import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.mods.MongoUtil;
import org.vertx.testtools.VertxAssert;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

public class MongoTypesTest extends PersistorTestParent {

  @Override
  protected JsonObject getConfig() {
    JsonObject config = super.getConfig();
    config.putBoolean("use_mongo_types", true);
    return config;
  }

  @Test
  public void validDatePersists() throws Exception {
    Date date = new Date();
    insertTypedData(date);
  }

  @Test
  public void validByteArrayPersists() throws Exception {
    byte[] data = new byte[]{1, 2, 3};
    insertTypedData(data);
  }

  @Test
  public void validArrayListPersists() throws Exception {
    List data = new ArrayList();
    data.add(1);
    data.add(2);
    data.add(new BasicDBObject("foo", "bar"));
    data.add(4);
    insertTypedData(data);
  }

  @Test
  public void validEmbeddedDocPersists() throws Exception {
    BasicDBObject y = new BasicDBObject("y", 3);
    BasicDBObject data = new BasicDBObject("x", y);
    insertTypedData(data);
  }

  @Test
  public void regexQueryWorks() throws Exception {
    deleteAll(new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        final String testValue = "{\"testKey\" : \"testValue\"}";

        JsonObject data = new JsonObject()
                .putObject("data", new JsonObject(testValue));

        JsonObject json = createSaveQuery(data);
        final DBObject dataDb = MongoUtil.convertJsonToBson(data);

        JsonObject matcher = new JsonObject()
                .putObject("data.testKey", new JsonObject()
                        .putString("$regex", ".*estValu.*"));

        JsonObject query = createMatcher(matcher);


        eb.send(ADDRESS, json, assertStored(query, dataDb, data));
      }
    });
  }

  @Test
  public void elemMatchQueryWorks() throws Exception {
    deleteAll(new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {

        List data = new ArrayList();
        data.add(1);
        data.add(2);
        data.add(4);

        final DBObject testValueDb = new BasicDBObject();
        testValueDb.put("data", data);
        JsonObject document = MongoUtil.convertBsonToJson(testValueDb);
        JsonObject json = createSaveQuery(document);
        final DBObject dataDb = MongoUtil.convertJsonToBson(document);

        JsonObject matcher = new JsonObject()
                .putObject("data", new JsonObject()
                        .putObject("$elemMatch", new JsonObject()
                                .putNumber("$gte", 0)
                                .putNumber("$lt", 5)));

        JsonObject query = createMatcher(matcher);


        eb.send(ADDRESS, json, assertStored(query, dataDb, data));
      }
    });
  }

  private void insertTypedData(final Object data) {
    deleteAll(new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        final String testValue = "{\"testKey\" : \"testValue\"}";
        final DBObject testValueDb = MongoUtil.convertJsonToBson(testValue);
        testValueDb.put("data", data);

        JsonObject document = MongoUtil.convertBsonToJson(testValueDb);
        JsonObject save = createSaveQuery(document);
        JsonObject matcher = new JsonObject(testValue);
        JsonObject query = createMatcher(matcher);

        eb.send(ADDRESS, save, assertStored(query, testValueDb, data));
      }
    });
  }

  private JsonObject createSaveQuery(JsonObject document) {
    return new JsonObject()
            .putString("collection", PersistorTestParent.COLLECTION)
            .putString("action", "save")
            .putObject("document", document);
  }

  private JsonObject createMatcher(JsonObject matcher) {
    return new JsonObject()
            .putString("collection", PersistorTestParent.COLLECTION)
            .putString("action", "find")
            .putObject("matcher", matcher);
  }

  private Handler<Message<JsonObject>> assertStored(final JsonObject query, final DBObject sentDbObject, final Object dataSaved) {
    return new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        assertEquals(reply.body().toString(), "ok", reply.body().getString("status"));

        eb.send(ADDRESS, query, new Handler<Message<JsonObject>>() {
                  public void handle(Message<JsonObject> reply) {
                    assertEquals(reply.body().toString(), "ok", reply.body().getString("status"));
                    JsonArray results = reply.body().getArray("results");

                    if (results.size() > 0) {
                      JsonObject result = results.get(0);
                      DBObject dbObj = MongoUtil.convertJsonToBson(result);
                      dbObj.removeField("_id");
                      Object storedData = dbObj.get("data");
                      if (storedData instanceof Binary) {
                        VertxAssert.assertArrayEquals((byte[]) dataSaved, ((Binary) storedData).getData());
                      } else {
                        VertxAssert.assertEquals(sentDbObject, dbObj);
                      }
                      testComplete();
                    } else {
                      VertxAssert.fail("Stored object not found in DB");
                    }
                  }
                }
        );
      }
    };
  }
}

