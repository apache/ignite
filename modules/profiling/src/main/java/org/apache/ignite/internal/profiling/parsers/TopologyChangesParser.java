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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.regex.Pattern.compile;

/** */
public class TopologyChangesParser implements IgniteLogParser {
    /** */
    private static final Pattern pattern = compile(
        "^pme \\[duration=\\d+, reason=.+tstamp=(\\d+).+, blocking=(true|false), resVer=.+]$");

    /** */
    private final ObjectNode res = mapper.createObjectNode();

    /** */
    long startTime;

    /** */
    long finishTime;

    /** */
    Set<String> nodeIds;

    /** */
    public TopologyChangesParser(Set<String> nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** {@inheritDoc} */
    @Override public void parse(String nodeId, String str) {
        if (!str.startsWith("pme"))
            return;

        Matcher matcher = pattern.matcher(str);

        if (!matcher.matches())
            return;

        long tstamp = Long.parseLong(matcher.group(1));

        startTime = startTime == 0 ? tstamp : Math.min(startTime, tstamp);

        finishTime = Math.max(finishTime, tstamp);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        res.put("startTime", startTime);
        res.put("finishTime", finishTime);

        ArrayNode nodeIds = mapper.createArrayNode();

        this.nodeIds.forEach(nodeIds::add);

        res.set("nodeIds", nodeIds);

        return U.map("topology", res);
    }
}
