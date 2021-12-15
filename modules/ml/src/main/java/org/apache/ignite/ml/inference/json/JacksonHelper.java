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

package org.apache.ignite.ml.inference.json;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

/** */
public class JacksonHelper {
    /** */
    public static void readAndValidateBasicJsonModelProperties(Path path, ObjectMapper mapper, String className) throws IOException {
        Map jsonAsMap = mapper.readValue(new File(path.toAbsolutePath().toString()), LinkedHashMap.class);
        String formatVersion = jsonAsMap.get("formatVersion").toString();
        Long timestamp = (Long)jsonAsMap.get("timestamp");
        String uid = jsonAsMap.get("uid").toString();
        String modelClass = jsonAsMap.get("modelClass").toString();

        if (!modelClass.equals(className)) {
            throw new IllegalArgumentException("You are trying to load " + modelClass + " model to " + className);
        }
    }
}
