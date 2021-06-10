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

package org.apache.ignite.configuration.json;

import java.util.List;
import com.google.gson.JsonElement;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;

public class JsonConverter {
    /**
     * @return Visitor to create JSON representations.
     */
    public static ConfigurationVisitor<JsonElement> jsonVisitor() {
        return new JsonConfigurationVisitor();
    }

    /**
     * @param jsonElement JSON that has to be converted to the configuration source.
     * @return JSON-based configuration source.
     */
    public static ConfigurationSource jsonSource(JsonElement jsonElement) {
        if (!jsonElement.isJsonObject())
            throw new IllegalArgumentException("JSON object is expected as a configuration source");

        return new JsonObjectConfigurationSource(List.of(), jsonElement.getAsJsonObject());
    }
}
