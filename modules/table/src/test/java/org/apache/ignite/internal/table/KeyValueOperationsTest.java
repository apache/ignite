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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic table operations test.
 * <p>
 * TODO: IGNITE-14487 Add bulk operations tests.
 * TODO: IGNITE-14487 Add async operations tests.
 */
public class KeyValueOperationsTest {
    /** Default mapper. */
    private final Mapper<Long> mapper = new Mapper<>() {
        @Override public Class<Long> getType() {
            return Long.class;
        }
    };

    /** Simple schema. */
    private SchemaDescriptor schema = new SchemaDescriptor(
        1,
        new Column[]{new Column("id", NativeTypes.INT64, false)},
        new Column[]{new Column("val", NativeTypes.INT64, false)}
    );

    /**
     * Creates table view.
     *
     * @return Table KV-view.
     */
    private KeyValueView<Long, Long> kvView() {
        return new KeyValueViewImpl<>(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), mapper, mapper, null);
    }

    /**
     *
     */
    @Test
    public void put() {
        KeyValueView<Long, Long> tbl = kvView();

        assertNull(tbl.get(1L));

        // Put KV pair.
        tbl.put(1L, 11L);

        assertEquals(11L, tbl.get(1L));
        assertEquals(11L, tbl.get(1L));

        // Update KV pair.
        tbl.put(1L, 22L);

        assertEquals(22L, tbl.get(1L));
        assertEquals(22L, tbl.get(1L));

        // Remove KV pair.
        tbl.put(1L, null);

        assertNull(tbl.get(1L));

        // Put KV pair.
        tbl.put(1L, 33L);
        assertEquals(33L, tbl.get(1L));
    }

    /**
     *
     */
    @Test
    public void putIfAbsent() {
        KeyValueView<Long, Long> tbl = kvView();

        assertNull(tbl.get(1L));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(1L, 11L));

        assertEquals(11L, tbl.get(1L));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(1L, 22L));

        assertEquals(11L, tbl.get(1L));
    }

    /**
     *
     */
    @Test
    public void getAndPut() {
        KeyValueView<Long, Long> tbl = kvView();

        assertNull(tbl.get(1L));

        // Insert new tuple.
        assertNull(tbl.getAndPut(1L, 11L));

        assertEquals(11L, tbl.get(1L));

        assertEquals(11L, tbl.getAndPut(1L, 22L));
        assertEquals(22L, tbl.getAndPut(1L, 33L));

        assertEquals(33L, tbl.get(1L));
    }

    /**
     *
     */
    @Test
    public void contains() {
        KeyValueView<Long, Long> tbl = kvView();

        // Not-existed value.
        assertFalse(tbl.contains(1L));

        // Put KV pair.
        tbl.put(1L, 11L);
        assertTrue(tbl.contains(1L));

        // Delete key.
        assertTrue(tbl.remove(1L));
        assertFalse(tbl.contains(1L));

        // Put KV pair.
        tbl.put(1L, 22L);
        assertTrue(tbl.contains(1L));

        // Delete key.
        tbl.remove(2L);
        assertFalse(tbl.contains(2L));
    }

    /**
     *
     */
    @Test
    public void remove() {
        KeyValueView<Long, Long> tbl = kvView();

        // Put KV pair.
        tbl.put(1L, 11L);

        // Delete existed key.
        assertEquals(11L, tbl.get(1L));
        assertTrue(tbl.remove(1L));
        assertNull(tbl.get(1L));

        // Delete already deleted key.
        assertFalse(tbl.remove(1L));

        // Put KV pair.
        tbl.put(1L, 22L);
        assertEquals(22L, tbl.get(1L));

        // Delete existed key.
        assertTrue(tbl.remove(1L));
        assertNull(tbl.get(1L));

        // Delete not existed key.
        assertNull(tbl.get(2L));
        assertFalse(tbl.remove(2L));
    }

    /**
     *
     */
    @Test
    public void removeExact() {
        KeyValueView<Long, Long> tbl = kvView();

        // Put KV pair.
        tbl.put(1L, 11L);
        assertEquals(11L, tbl.get(1L));

        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(1L, 22L));
        assertEquals(11L, tbl.get(1L));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(1L, 11L));
        assertNull(tbl.get(1L));

        // Once again.
        assertFalse(tbl.remove(1L, 11L));
        assertNull(tbl.get(1L));

        // Try to remove non-existed key.
        assertFalse(tbl.remove(1L, 11L));
        assertNull(tbl.get(1L));

        // Put KV pair.
        tbl.put(1L, 22L);
        assertEquals(22L, tbl.get(1L));

        // Check null value ignored.
        assertThrows(Throwable.class, () -> tbl.remove(1L, null));
        assertEquals(22L, tbl.get(1L));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(1L, 22L));
        assertNull(tbl.get(1L));

        assertFalse(tbl.remove(2L, 22L));
        assertNull(tbl.get(2L));
    }

    /**
     *
     */
    @Test
    public void replace() {
        KeyValueView<Long, Long> tbl = kvView();

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(1L, 11L));
        assertNull(tbl.get(1L));

        tbl.put(1L, 11L);

        // Replace existed KV pair.
        assertTrue(tbl.replace(1L, 22L));
        assertEquals(22L, tbl.get(1L));

        // Remove existed KV pair.
        assertTrue(tbl.replace(1L, null));
        assertNull(tbl.get(1L));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(1L, 33L));
        assertNull(tbl.get(1L));

        tbl.put(1L, 33L);
        assertEquals(33L, tbl.get(1L));

        // Remove non-existed KV pair.
        assertFalse(tbl.replace(2L, null));
        assertNull(tbl.get(2L));
    }

    /**
     *
     */
    @Test
    public void replaceExact() {
        KeyValueView<Long, Long> tbl = kvView();

        // Insert KV pair.
        assertTrue(tbl.replace(1L, null, 11L));
        assertEquals(11L, tbl.get(1L));
        assertNull(tbl.get(2L));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(2L, 11L, 22L));
        assertNull(tbl.get(2L));

        // Replace existed KV pair.
        assertTrue(tbl.replace(1L, 11L, 22L));
        assertEquals(22L, tbl.get(1L));

        // Remove existed KV pair.
        assertTrue(tbl.replace(1L, 22L, null));
        assertNull(tbl.get(1L));

        // Insert KV pair.
        assertTrue(tbl.replace(1L, null, 33L));
        assertEquals(33L, tbl.get(1L));

        // Remove non-existed KV pair.
        assertTrue(tbl.replace(2L, null, null));
    }
}
