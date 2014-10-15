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
  public void testCommand() throws Exception {
    JsonObject ping = new JsonObject()
            .putString("action", "command")
            .putString("command", "{ping:1}");

    eb.send(ADDRESS, ping, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        Number ok = reply.body()
                .getObject("result")
                .getNumber("ok");

        assertEquals(1.0, ok);
        testComplete();
      }
    });
  }


  @Test
  public void testDate() throws Exception {
    Date date = new Date();
    insertTypedData(date);
  }

  @Test
  public void testByteArray() throws Exception {
    byte[] data = new byte[]{1, 2, 3};
    insertTypedData(data);
  }

  @Test
  public void testArrayList() throws Exception {
    List data = new ArrayList();
    data.add(1);
    data.add(2);
    data.add(new BasicDBObject("foo", "bar"));
    data.add(4);
    insertTypedData(data);
  }

  @Test
  public void testEmbeddedDoc() throws Exception {
    BasicDBObject y = new BasicDBObject("y", 3);
    BasicDBObject data = new BasicDBObject("x", y);
    insertTypedData(data);
  }

  private void insertTypedData(final Object data) {
    deleteAll(new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        final String testValue = "{\"testKey\" : \"testValue\"}";
        final DBObject obj = MongoUtil.convertJsonToBson(testValue);
        obj.put("data", data);

        JsonObject docWithDate = MongoUtil.convertBsonToJson(obj);

        JsonObject json = new JsonObject()
                .putString("collection", COLLECTION)
                .putString("action", "save").putObject("document", docWithDate);

        eb.send(ADDRESS, json, new Handler<Message<JsonObject>>() {
          public void handle(Message<JsonObject> reply) {
            assertEquals(reply.body().toString(), "ok", reply.body().getString("status"));

            assertStored(testValue, data, obj);
          }
        });
      }
    });

  }

  private void assertStored(String testValue, final Object data, final DBObject sentObject) {
    JsonObject matcher = new JsonObject(testValue);
    JsonObject query = new JsonObject()
            .putString("collection", COLLECTION)
            .putString("action", "find")
            .putObject("matcher", matcher);

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
                    VertxAssert.assertArrayEquals((byte[]) data, ((Binary) storedData).getData());
                  } else {
                    VertxAssert.assertEquals(sentObject, dbObj);
                  }
                  testComplete();
                } else {
                  VertxAssert.fail("Stored object not found in DB");
                }
              }
            }

    );
  }
}

