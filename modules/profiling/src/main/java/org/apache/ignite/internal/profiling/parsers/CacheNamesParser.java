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

package org.apache.ignite.internal.profiling.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** */
public class CacheNamesParser implements IgniteLogParser {
    /** */
    private static final Pattern pattern = Pattern.compile(
        "^cacheStart \\[id=(-?\\d+), startTime=(\\d+), name=(.*), group=(.*), userCache=(true|false)]$");

    /** */
    private final ObjectNode res = mapper.createObjectNode();

    /** {@inheritDoc} */
    @Override public void parse(String nodeId, String str) {
        if (!str.startsWith("cacheStart"))
            return;

        Matcher matcher = pattern.matcher(str);

        if (!matcher.matches())
            return;

        int cacheId = Integer.parseInt(matcher.group(1));

        if (res.findValue(String.valueOf(cacheId)) != null)
            return;

        long startTime = Long.parseLong(matcher.group(2));
        String cacheName = matcher.group(3);
        String groupName = matcher.group(4);
        boolean userCache = Boolean.parseBoolean(matcher.group(5));

        ObjectNode node = mapper.createObjectNode();

        node.put("cacheId", cacheId);
        node.put("startTime", startTime);
        node.put("cacheName", cacheName);
        node.put("groupName", groupName);
        node.put("userCache", userCache);

        res.set(String.valueOf(cacheId), node);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        return Collections.singletonMap("startedCaches", res);
    }
}
