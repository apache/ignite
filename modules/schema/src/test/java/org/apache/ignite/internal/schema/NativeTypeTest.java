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

import org.apache.ignite.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class NativeTypeTest {
    /**
     *
     */
    @Test
    public void compareFixlenTypesVsVarlenTypes() {
        assertTrue(INT8.compareTo(STRING) < 0);
        assertTrue(INT8.compareTo(BYTES) < 0);

        assertTrue(NativeTypes.INT32.compareTo(STRING) < 0);
        assertTrue(NativeTypes.INT32.compareTo(BYTES) < 0);

        assertTrue(NativeTypes.INT64.compareTo(STRING) < 0);
        assertTrue(NativeTypes.INT64.compareTo(BYTES) < 0);

        assertTrue(NativeTypes.UUID.compareTo(STRING) < 0);
        assertTrue(NativeTypes.UUID.compareTo(BYTES) < 0);
    }

    /**
     *
     */
    @Test
    public void compareFixlenTypesBySize() {
        assertTrue(NativeTypes.INT16.compareTo(NativeTypes.INT32) < 0);
        assertTrue(NativeTypes.INT32.compareTo(NativeTypes.INT64) < 0);
        assertTrue(NativeTypes.INT64.compareTo(NativeTypes.UUID) < 0);
    }

    /**
     *
     */
    @Test
    public void compareFixlenTypesByDesc() {
        assertTrue(NativeTypes.FLOAT.compareTo(NativeTypes.INT32) < 0);
    }

    /**
     *
     */
    @Test
    public void compareVarlenTypesByDesc() {
        assertTrue(BYTES.compareTo(STRING) < 0);
    }

    /**
     *
     */
    @Test
    public void createNativeTypeFromColumnType() {
        assertEquals(INT8, NativeTypes.from(ColumnType.INT8));
        assertEquals(INT16, NativeTypes.from(ColumnType.INT16));
        assertEquals(INT32, NativeTypes.from(ColumnType.INT32));
        assertEquals(INT64, NativeTypes.from(ColumnType.INT64));
        assertEquals(FLOAT, NativeTypes.from(ColumnType.FLOAT));
        assertEquals(DOUBLE, NativeTypes.from(ColumnType.DOUBLE));
        assertEquals(BYTES, NativeTypes.from(ColumnType.blobOf()));
        assertEquals(STRING, NativeTypes.from(ColumnType.string()));

        for (int i = 1; i < 800; i += 100) {
            assertEquals(NativeTypes.blobOf(i), NativeTypes.from(ColumnType.blobOf(i)));
            assertEquals(NativeTypes.stringOf(i), NativeTypes.from(ColumnType.stringOf(i)));
            assertEquals(NativeTypes.bitmaskOf(i), NativeTypes.from(ColumnType.bitmaskOf(i)));
        }
    }
}
