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

package org.apache.ignite.internal.schema;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class SchemaDescriptorTest {
    /**
     *
     */
    @Test
    public void columnIndexedAccess() {
        SchemaDescriptor desc = new SchemaDescriptor(
                1,
                new Column[]{
                        new Column("columnA", NativeTypes.INT8, false),
                        new Column("columnB", NativeTypes.UUID, false),
                        new Column("columnC", NativeTypes.INT32, false),
                },
                new Column[]{
                        new Column("columnD", NativeTypes.INT8, false),
                        new Column("columnE", NativeTypes.UUID, false),
                        new Column("columnF", NativeTypes.INT32, false),
                }
        );

        assertEquals(6, desc.length());

        for (int i = 0; i < desc.length(); i++) {
            assertEquals(i, desc.column(i).schemaIndex());
        }
    }

    /**
     * Check column order.
     */
    @Test
    public void columnOrder() {
        Column[] keyColumns = {
                new Column(0, "columnA", NativeTypes.INT8, false, () -> null),
                new Column(1, "columnB", NativeTypes.UUID, false, () -> null),
                new Column(2, "columnC", NativeTypes.INT32, false, () -> null),
        };

        Column[] valColumns = {
                new Column(3, "columnD", NativeTypes.INT8, false, () -> null),
                new Column(4, "columnE", NativeTypes.UUID, false, () -> null),
                new Column(5, "columnF", NativeTypes.INT32, false, () -> null),
        };

        List<Column> columns = new ArrayList<>();
        Collections.addAll(columns, keyColumns);
        Collections.addAll(columns, valColumns);

        SchemaDescriptor desc = new SchemaDescriptor(1, keyColumns, valColumns);

        assertEquals(6, desc.length());

        for (int i = 0; i < columns.size(); i++) {
            Column col = desc.column(i);

            assertEquals(columns.get(col.columnOrder()), col);
        }

        assertArrayEquals(columns.stream().map(Column::name).toArray(String[]::new), desc.columnNames().toArray(String[]::new));
    }
}
