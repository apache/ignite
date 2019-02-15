/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
