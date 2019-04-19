/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.stream.flume;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.Event;
import org.jetbrains.annotations.Nullable;

/**
 * A test transformer to convert {@link org.apache.flume.Event} to cacheable entries.
 */
public class TestEventTransformer implements EventTransformer<Event, String, Integer> {

    /**
     * Transforms a Flume event to cacheable entries.
     *
     * @param event Flume event to transform.
     * @return Map of cacheable entries.
     */
    private Map<String, Integer> transform(Event event) {
        final Map<String, Integer> map = new HashMap<>();

        String eventStr = new String(event.getBody());

        if (!eventStr.isEmpty()) {
            String[] tokens = eventStr.split(":"); // Expects column-delimited one line.

            map.put(tokens[0].trim(), Integer.valueOf(tokens[1].trim()));
        }

        return map;
    }

    /**
     * Transforms a list of Flume event to cacheable entries.
     *
     * @param events Flume events to transform.
     * @return Map of cacheable entries.
     */
    @Nullable @Override public Map<String, Integer> transform(List<Event> events) {
        final Map<String, Integer> map = new HashMap<>();

        for (Event event : events) {
            map.putAll(transform(event));
        }

        return map;
    }
}
