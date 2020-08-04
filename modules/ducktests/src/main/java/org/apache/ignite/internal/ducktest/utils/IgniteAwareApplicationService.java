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

package org.apache.ignite.internal.ducktest.utils;

import java.util.Base64;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *
 */
public class IgniteAwareApplicationService {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(IgniteAwareApplicationService.class.getName());

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        log.info("Starting Application... [params=" + args[0] + "]");

        String[] params = args[0].split(",");

        Class<?> clazz = Class.forName(params[0]);

        IgniteAwareApplication app = (IgniteAwareApplication)clazz.getConstructor().newInstance();

        ObjectMapper mapper = new ObjectMapper();

        ObjectReader reader = mapper.readerFor(Map.class);

        JsonNode jsonNode = params.length > 2 ?
            reader.readTree(new String(Base64.getDecoder().decode(params[2]), UTF_8)) : mapper.createObjectNode();

        ((ObjectNode)jsonNode).put("cfgPath", params[1]);

        app.start(jsonNode);
    }
}
