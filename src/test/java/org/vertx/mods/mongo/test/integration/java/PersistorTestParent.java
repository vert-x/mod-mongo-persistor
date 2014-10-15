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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

public abstract class PersistorTestParent extends TestVerticle {

  public static final String ADDRESS = "test.persistor";
  public static final String COLLECTION = "testcoll";
  protected EventBus eb;

  @Override
  public void start() {
    eb = vertx.eventBus();
    JsonObject config = getConfig();
    container.deployModule(System.getProperty("vertx.modulename"), config, 1, new AsyncResultHandler<String>() {
      public void handle(AsyncResult<String> ar) {
        if (ar.succeeded()) {
          PersistorTestParent.super.start();
        } else {
          ar.cause().printStackTrace();
        }
      }
    });
  }

  protected JsonObject getConfig() {
    JsonObject config = new JsonObject();
    config.putString("address", ADDRESS);
    config.putString("db_name", System.getProperty("vertx.mongo.database", "test_db"));
    config.putString("host", System.getProperty("vertx.mongo.host", "localhost"));
    config.putNumber("port", Integer.valueOf(System.getProperty("vertx.mongo.port", "27017")));
    config.putBoolean("use_mongo_types", true);
    String username = System.getProperty("vertx.mongo.username");
    String password = System.getProperty("vertx.mongo.password");
    if (username != null) {
      config.putString("username", username);
      config.putString("password", password);
    }
    config.putBoolean("fake", false);
    return config;
  }

  ;

  protected void deleteAll(Handler<Message<JsonObject>> handler) {
    JsonObject matcher = new JsonObject("{}");
    JsonObject query = new JsonObject()
            .putString("collection", COLLECTION)
            .putString("action", "delete")
            .putObject("matcher", matcher);

    eb.send(ADDRESS, query, handler);
  }
}

