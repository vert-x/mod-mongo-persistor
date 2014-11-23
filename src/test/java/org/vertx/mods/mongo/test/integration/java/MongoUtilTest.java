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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;
import org.vertx.mods.MongoUtil;

import java.util.Date;

import static org.junit.Assert.assertTrue;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 10/9/14
 */
public class MongoUtilTest {
  @Test
  public void testConvertBsonToJson() {
    String testValue = "{\"testKey\" : \"testValue\"}";
    DBObject obj = MongoUtil.convertJsonToBson(testValue);
    Date date = new Date();
    obj.put("created", date);

    JsonObject json = MongoUtil.convertBsonToJson(obj);
    JsonObject value = json.getValue("created");
    assertTrue(value.getLong("$date") == date.getTime());

  }

  @Test
  public void testConvertJsonToBson() {
    ObjectId id = new ObjectId();
    DBObject obj = new BasicDBObject();
    obj.put("id", id);
    Date date = new Date();
    obj.put("created", date);
    Integer[] numbers = new Integer[]{1, 2, 3};
    obj.put("values", numbers);

    JsonObject jsonObject = MongoUtil.convertBsonToJson(obj);
    DBObject convertedObj = MongoUtil.convertJsonToBson(jsonObject);

    assertTrue(convertedObj.get("id").equals(id));
    BasicDBList list = (BasicDBList) convertedObj.get("values");

    for (int i = 0; i < list.size(); i++) {
      assertTrue(list.get(i) == numbers[i]);
    }
    assertTrue(convertedObj.get("created").equals(date));
  }
}
