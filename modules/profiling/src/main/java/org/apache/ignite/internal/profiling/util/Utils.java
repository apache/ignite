/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.profiling.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** */
public class Utils {
    /** Json mapper. */
    public static final ObjectMapper MAPPER = new ObjectMapper();

    /** Creates empty object for given value if absent. */
    public static ObjectNode createObjectIfAbsent(String val, ObjectNode json) {
        ObjectNode node = (ObjectNode)json.get(val);

        if (node == null) {
            node = MAPPER.createObjectNode();

            json.set(val, node);
        }

        return node;
    }

    /** Creates empty array for given value if absent. */
    public static ArrayNode createArrayIfAbsent(String val, ObjectNode json) {
        ArrayNode node = (ArrayNode)json.get(val);

        if (node == null) {
            node = MAPPER.createArrayNode();

            json.set(val, node);
        }

        return node;
    }

}
