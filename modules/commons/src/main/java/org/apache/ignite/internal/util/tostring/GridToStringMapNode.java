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

package org.apache.ignite.internal.util.tostring;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.COLLECTION_LIMIT;
import static org.apache.ignite.internal.util.tostring.GridToStringNodeFactory.getGridToStringNode;

/**
 * A node that represents a map (key-value pairs) in the string representation.
 * It creates nodes for each key and value and formats the output with curly braces.
 */
class GridToStringMapNode extends NodeRecursionMonitor {
    /** An internal map that stores key-value pairs as GridToStringNode instances. */
    private final Map<GridToStringNode, GridToStringNode> map;

    /** The simple class name of the map being represented. */
    private final String mapClsSimpleName;

    /** The rule for appending a hint about skipped elements if the map is too large. */
    private final LongSequenceSkipRule skipRule;

    /**
     * Constructs a new map node.
     * Iterates over the entries of the input map, creates nodes for keys and values,
     * and populates the internal map structure up to the collection size limit.
     * @param propName The property name.
     * @param map The map to represent.
     */
    GridToStringMapNode(String propName, Map<?, ?> map) {
        super(propName, map);
        try {
            acquireRecursionMonitor();
            mapClsSimpleName = map.getClass().getSimpleName();
            this.map = new HashMap<>();
            skipRule = new LongSequenceSkipRule(map::size);
            Iterator<? extends Map.Entry<?, ?>> iter = map.entrySet().iterator();
            while (iter.hasNext() && this.map.size() != COLLECTION_LIMIT) {
                Map.Entry<?, ?> entry = iter.next();
                Object key = entry.getKey();
                Object val = entry.getValue();
                GridToStringNode keyNode = getGridToStringNode(null, () -> key, key::getClass);
                GridToStringNode valNode = getGridToStringNode(null, () -> val, val::getClass);
                this.map.put(keyNode, valNode);
            }
        }
        finally {
            releaseRecursionMonitor();
        }
    }

    /**
     * Appends the string representation of the map to the builder.
     * The format is: MapClassName {key1=value1, key2=value2, ...}.
     * Also appends a hint about skipped elements if necessary.
     * @param sb The string builder to append to.
     */
    @Override void appendNode(GridStringBuilder sb) {
        super.appendNode(sb);
        sb.a(mapClsSimpleName).a(" {");
        Iterator<Map.Entry<GridToStringNode, GridToStringNode>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<GridToStringNode, GridToStringNode> entry = iter.next();
            entry.getKey().appendNode(sb);
            sb.a('=');
            entry.getValue().appendNode(sb);
            if (iter.hasNext())
                sb.a(", ");
        }
        skipRule.appendSkippedCountHint(sb);
        sb.a('}');
    }
}
