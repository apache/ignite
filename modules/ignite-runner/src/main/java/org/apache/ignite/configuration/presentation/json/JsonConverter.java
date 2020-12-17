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

package org.apache.ignite.configuration.presentation.json;

import java.io.Reader;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.configuration.presentation.FormatConverter;

/** */
public class JsonConverter implements FormatConverter {
    /** */
    private final Gson gson = new Gson();

    /** {@inheritDoc} */
    @Override public String convertTo(Object obj) {
        return gson.toJson(obj);
    }

    /** {@inheritDoc} */
    @Override public String convertTo(String rootName, Object src) {
        Map<String, Object> res = new HashMap<>();

        res.put(rootName, src);

        return gson.toJson(res);
    }

    /** {@inheritDoc} */
    @Override public <T> T convertFrom(String source, String rootName, Class<T> clazz) {
        Map map = gson.fromJson(source, Map.class);

        String root = gson.toJson(map.get(rootName));

        return gson.fromJson(root, clazz);
    }

    /** {@inheritDoc} */
    @Override public <T> T convertFrom(Reader source, String rootName, Class<T> clazz) {
        Map map = gson.fromJson(source, Map.class);

        String root = gson.toJson(map.get(rootName));

        return gson.fromJson(root, clazz);
    }
}
