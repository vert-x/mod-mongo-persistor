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

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONSerializers;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.json.impl.Json;

import java.util.HashMap;

/**
 * Utilities for converting Bson objects to and from vert.x JsonObject objects
 *
 * @author Jeremy Truelove
 */
public class MongoUtil {

    /**
     * Converts a JsonObject to a MongoDB object
     * @param json the vert.x JsonObject to convert
     * @return the converted DBObject
     * @throws java.lang.IllegalArgumentException if you pass in a null object
     */
    public static DBObject convertJsonToBson(JsonObject json) {
        if (json == null) {
            throw new IllegalArgumentException("Cannot convert null object to DBObject");
        }

        return convertJsonToBson(json.encode());
    }

    /**
     * Takes a Json string and converts it to a MongoDB DBObject
     *
     * @param json the json string to convert into a MongoDB object
     * @return the converted DBObject
     * @throws java.lang.IllegalArgumentException if you pass in a null object
     */
    public static DBObject convertJsonToBson(String json) {
        if (json == null || json.equals("")) {
            throw new IllegalArgumentException("Cannot convert empty string to DBObject");
        }

        return  (DBObject) JSON.parse(json);
    }

    /**
     * Converts a DBObject to its Bson form and then encapsulates it in a JsonObject
     * @param dbObject the object to convert
     * @return the JsonObject representing the Bson MongoDB form
     * @throws java.lang.IllegalArgumentException if you pass in a null object
     */
    public static JsonObject convertBsonToJson(DBObject dbObject) {
        if (dbObject == null) {
            throw new IllegalArgumentException("Cannot convert null to JsonObject");
        }

        // Create JSON string from DBObject
        String serialize = JSONSerializers.getStrict().serialize(dbObject);

        // Convert to JsonObject
        HashMap<String, Object> jsonMap = Json.decodeValue(serialize, HashMap.class);
        return new JsonObject(jsonMap);
    }
}
