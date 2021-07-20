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

package org.apache.ignite.internal.configuration.tree;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test with basic {@link OrderedMap} invariants. */
public class OrderedMapTest {
    /** Map instance. */
    private final OrderedMap<String> map = new OrderedMap<>();

    /** Tests put/get/remove consistency on a single key. */
    @Test
    public void putGetRemove() {
        assertNull(map.get("key1"));

        map.put("key", "value");

        assertEquals(1, map.size());
        assertEquals("value", map.get("key"));

        map.remove("key");

        assertNull(map.get("key1"));
    }

    /** Tests that {@link OrderedMap#put(String, Object)} preserves order. */
    @Test
    public void keysOrder() {
        map.put("key1", "value1");
        map.put("key2", "value2");

        assertEquals(2, map.size());
        assertEquals(List.of("key1", "key2"), map.keys());

        map.clear();

        assertEquals(0, map.size());
        assertEquals(List.of(), map.keys());

        map.put("key2", "value2");
        map.put("key1", "value1");

        assertEquals(2, map.size());
        assertEquals(List.of("key2", "key1"), map.keys());

        map.put("key2", "value3");
        assertEquals(List.of("key2", "key1"), map.keys());
    }

    /** Tests that {@link OrderedMap#putByIndex(int, String, Object)} preserves order. */
    @Test
    public void putByIndex() {
        map.putByIndex(0, "key1", "value1");

        map.putByIndex(0, "key2", "value2");

        assertEquals(List.of("key2", "key1"), map.keys());

        map.putByIndex(1, "key3", "value3");

        assertEquals(List.of("key2", "key3", "key1"), map.keys());

        map.putByIndex(3, "key4", "value4");

        assertEquals(List.of("key2", "key3", "key1", "key4"), map.keys());

        map.putByIndex(5, "key5", "value5");

        assertEquals(List.of("key2", "key3", "key1", "key4", "key5"), map.keys());
    }

    /** Tests that {@link OrderedMap#putAfter(String, String, Object)} preserves order. */
    @Test
    public void putAfter() {
        map.put("key1", "value1");

        assertEquals(List.of("key1"), map.keys());

        map.putAfter("key1", "key2", "value2");

        assertEquals(List.of("key1", "key2"), map.keys());

        map.putAfter("key1", "key3", "value3");

        assertEquals(List.of("key1", "key3", "key2"), map.keys());

        map.putAfter("key2", "key4", "value4");

        assertEquals(List.of("key1", "key3", "key2", "key4"), map.keys());
    }

    /** Tests basic invariants of {@link OrderedMap#rename(String, String)} method. */
    @Test
    public void rename() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        map.rename("key2", "key4");

        assertEquals("value2", map.get("key4"));

        assertFalse(map.containsKey("key2"));

        assertEquals(List.of("key1", "key4", "key3"), map.keys());
    }

    /** Tests that {@link OrderedMap#reorderKeys(List)} reorders keys properly. */
    @Test
    public void reorderKeys() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        assertEquals(List.of("key1", "key2", "key3"), map.keys());

        map.reorderKeys(List.of("key2", "key1", "key3"));

        assertEquals(List.of("key2", "key1", "key3"), map.keys());

        map.reorderKeys(List.of("key2", "key3", "key1"));

        assertEquals(List.of("key2", "key3", "key1"), map.keys());

        map.reorderKeys(List.of("key1", "key3", "key2"));

        assertEquals(List.of("key1", "key3", "key2"), map.keys());
    }
}
