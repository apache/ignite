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
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Check data by strict schema.
 */
public class StrictSchemaOperationsTest {
    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /**
     *
     */
    @Test
    public void columnNotExist() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, true)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        assertThrows(SchemaMismatchException.class, () -> tbl.insert(Tuple.create().set("id", 0L).set("invalidCol", 0)));
    }

    /**
     *
     */
    @Test
    public void schemaMismatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, true)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        assertThrows(SchemaMismatchException.class, () -> tbl.get(Tuple.create().set("id", 0L).set("val", 0L)));

        assertThrows(SchemaMismatchException.class, () -> tbl.kvView().get(Tuple.create().set("id", 0L).set("val", 0L)));
        assertThrows(SchemaMismatchException.class, () -> tbl.kvView().put(Tuple.create().set("id", 0L).set("val", 0L), Tuple.create()));
        assertThrows(SchemaMismatchException.class, () -> tbl.kvView().put(Tuple.create().set("id", 0L), Tuple.create().set("id", 0L).set("val", 0L)));
    }

    /**
     *
     */
    @Test
    public void typeMismatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {
                new Column("valString", NativeTypes.stringOf(3), true),
                new Column("valBytes", NativeTypes.blobOf(3), true)
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        // Check not-nullable column.
        assertThrows(IllegalArgumentException.class, () -> tbl.insert(Tuple.create().set("id", null)));

        // Check length of the string column
        assertThrows(InvalidTypeException.class, () -> tbl.insert(Tuple.create().set("id", 0L).set("valString", "qweqwe")));

        // Check length of the string column
        assertThrows(InvalidTypeException.class, () -> tbl.insert(Tuple.create().set("id", 0L).set("valBytes", new byte[]{0, 1, 2, 3})));
    }

    /**
     *
     */
    @Test
    public void stringTypeMatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {
                new Column("valString", NativeTypes.stringOf(3), true)
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        Tuple tuple = Tuple.create().set("id", 1L);

        tbl.insert(tuple.set("valString", "qwe"));
        tbl.insert(tuple.set("valString", "qw"));
        tbl.insert(tuple.set("valString", "q"));
        tbl.insert(tuple.set("valString", ""));
        tbl.insert(tuple.set("valString", null));

        // Check string 3 char length and 9 bytes.
        tbl.insert(tuple.set("valString", "我是谁"));
    }

    /**
     *
     */
    @Test
    public void bytesTypeMatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {
                new Column("valUnlimited", NativeTypes.BYTES, true),
                new Column("valLimited", NativeTypes.blobOf(2), true)
            });

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        Tuple tuple = Tuple.create().set("id", 1L);

        tbl.insert(tuple.set("valUnlimited", null));
        tbl.insert(tuple.set("valLimited", null));
        tbl.insert(tuple.set("valUnlimited", new byte[2]));
        tbl.insert(tuple.set("valLimited", new byte[2]));
        tbl.insert(tuple.set("valUnlimited", new byte[3]));

        assertThrows(InvalidTypeException.class, () -> tbl.insert(tuple.set("valLimited", new byte[3])));

    }
}
