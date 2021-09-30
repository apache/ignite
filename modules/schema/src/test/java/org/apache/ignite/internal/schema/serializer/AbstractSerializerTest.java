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

package org.apache.ignite.internal.schema.serializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.marshaller.schema.AbstractSchemaSerializer;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * SchemaDescriptor (de)serializer test.
 */
public class AbstractSerializerTest {
    /**
     * (de)Serialize schema test.
     */
    @Test
    public void schemaSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
            new Column[] {
                new Column("A", NativeTypes.INT8, false),
                new Column("B", NativeTypes.INT16, false),
                new Column("C", NativeTypes.INT32, false),
                new Column("D", NativeTypes.INT64, false),
                new Column("E", NativeTypes.UUID, false),
                new Column("F", NativeTypes.FLOAT, false),
                new Column("G", NativeTypes.DOUBLE, false),
                new Column("H", NativeTypes.DATE, false),
            },
            new Column[] {
                new Column("A1", NativeTypes.stringOf(128), false),
                new Column("B1", NativeTypes.numberOf(255), false),
                new Column("C1", NativeTypes.decimalOf(128, 64), false),
                new Column("D1", NativeTypes.bitmaskOf(256), false),
                new Column("E1", NativeTypes.datetime(8), false),
                new Column("F1", NativeTypes.time(8), false),
                new Column("G1", NativeTypes.timestamp(8), true)
            }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        assertEquals(desc.version(), deserialize.version());

        assertArrayEquals(desc.keyColumns().columns(), deserialize.keyColumns().columns());
        assertArrayEquals(desc.valueColumns().columns(), deserialize.valueColumns().columns());
        assertArrayEquals(desc.affinityColumns(), deserialize.affinityColumns());
    }

    /**
     * (de)Serialize default value test.
     */
    @Test
    public void defaultValueSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
            new Column[] {
                new Column("A", NativeTypes.INT8, false, () -> (byte)1),
                new Column("B", NativeTypes.INT16, false, () -> (short)1),
                new Column("C", NativeTypes.INT32, false, () -> 1),
                new Column("D", NativeTypes.INT64, false, () -> 1L),
                new Column("E", NativeTypes.UUID, false, () -> new UUID(12,34)),
                new Column("F", NativeTypes.FLOAT, false, () -> 1.0f),
                new Column("G", NativeTypes.DOUBLE, false, () -> 1.0d),
                new Column("H", NativeTypes.DATE, false),
            },
            new Column[] {
                new Column("A1", NativeTypes.stringOf(128), false, () -> "test"),
                new Column("B1", NativeTypes.numberOf(255), false, () -> BigInteger.TEN),
                new Column("C1", NativeTypes.decimalOf(128, 64), false, () -> BigDecimal.TEN),
                new Column("D1", NativeTypes.bitmaskOf(256), false, BitSet::new)
            }
        );

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        //key columns
        assertEquals(deserialize.column("A").defaultValue(), (byte)1);
        assertEquals(deserialize.column("B").defaultValue(), (short)1);
        assertEquals(deserialize.column("C").defaultValue(), 1);
        assertEquals(deserialize.column("D").defaultValue(), 1L);
        assertEquals(deserialize.column("E").defaultValue(), new UUID(12,34));
        assertEquals(deserialize.column("F").defaultValue(), 1.0f);
        assertEquals(deserialize.column("G").defaultValue(), 1.0d);
        assertNull(deserialize.column("H").defaultValue());

        //value columns
        assertEquals(deserialize.column("A1").defaultValue(), "test");
        assertEquals(deserialize.column("B1").defaultValue(), BigInteger.TEN);
        assertEquals(deserialize.column("C1").defaultValue(), BigDecimal.TEN);
        assertEquals(deserialize.column("D1").defaultValue(), new BitSet());
    }

    /**
     * (de)Serialize column mapping test.
     */
    @Test
    public void columnMappingSerializeTest() {
        AbstractSchemaSerializer assembler = SchemaSerializerImpl.INSTANCE;

        SchemaDescriptor desc = new SchemaDescriptor(100500,
            new Column[] {
                new Column("A", NativeTypes.INT8, false, () -> (byte)1)
            },
            new Column[] {
                new Column("A1", NativeTypes.stringOf(128), false, () -> "test"),
                new Column("B1", NativeTypes.numberOf(255), false, () -> BigInteger.TEN)
            }
        );

        ColumnMapper mapper = ColumnMapping.createMapper(desc);

        mapper.add(0, 1);

        Column c1 = new Column("C1", NativeTypes.stringOf(128), false, () -> "brandNewColumn").copy(2);

        mapper.add(c1);

        desc.columnMapping(mapper);

        byte[] serialize = assembler.serialize(desc);

        SchemaDescriptor deserialize = assembler.deserialize(serialize);

        ColumnMapper mapper1 = deserialize.columnMapping();

        assertEquals(1, mapper1.map(0));
        assertEquals(c1, mapper1.mappedColumn(2));
    }
}
