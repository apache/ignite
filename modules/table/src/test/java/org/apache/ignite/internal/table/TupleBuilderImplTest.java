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

import java.util.UUID;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests server tuple builder implementation.
 *
 * Should be in sync with org.apache.ignite.client.ClientTupleBuilderTest.
 */
public class TupleBuilderImplTest {
    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(UUID.randomUUID(), 1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("name", NativeTypes.STRING, true)
    });

    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) getTuple().value("id"));
        assertEquals("Shirt", getTuple().value("name"));
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long) getTuple().value(0));
        assertEquals("Shirt", getTuple().value(1));
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, getTuple().valueOrDefault("id", -1L));
        assertEquals("Shirt", getTuple().valueOrDefault("name", "y"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotPresent() {
        assertEquals("foo", getBuilder().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsPresentButNotSet() {
        assertEquals("foo", getBuilder().valueOrDefault("name", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        var tuple = getBuilder().set("name", null).build();

        assertNull(tuple.valueOrDefault("name", "foo"));
    }

    @Test
    public void testSetThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(IgniteException.class, () -> getBuilder().set("x", "y"));
        assertTrue(ex.getMessage().startsWith("Column not found"), ex.getMessage());
    }

    @Test
    public void testValueThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(IgniteException.class, () -> getBuilder().value("x"));
        assertTrue(ex.getMessage().startsWith("Column not found"), ex.getMessage());

        var ex2 = assertThrows(IllegalArgumentException.class, () -> getBuilder().value(100));
        assertTrue(ex2.getMessage().startsWith("Column index can't be greater than 1"), ex2.getMessage());
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(SCHEMA.length(), getTuple().columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("id", getTuple().columnName(0));
        assertEquals("name", getTuple().columnName(1));
    }

    @Test
    public void testColumnNameThrowsOnInvalidIndex() {
        var ex = assertThrows(IllegalArgumentException.class, () -> getTuple().columnName(-1));
        assertEquals("Column index can't be negative", ex.getMessage());
    }

    @Test
    public void testColumnIndexReturnsIndexByName() {
        assertEquals(0, getTuple().columnIndex("id"));
        assertEquals(1, getTuple().columnIndex("name"));
    }

    @Test
    public void testColumnIndexReturnsNullForMissingColumns() {
        assertNull(getTuple().columnIndex("foo"));
    }

    private static TupleBuilderImpl getBuilder() {
        return new TupleBuilderImpl(SCHEMA);
    }

    private static Tuple getTuple() {
        return new TupleBuilderImpl(SCHEMA)
                .set("id", 3L)
                .set("name", "Shirt")
                .build();
    }
}
