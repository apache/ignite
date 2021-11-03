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

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class ColumnDefinitionTest {
    /**
     *
     */
    @Test
    public void compareColumns() {
        Column[] cols = new Column[]{
                new Column("C", BYTES, false),
                new Column("B", INT32, false),
                new Column("A", DATE, false),
                new Column("AD", STRING, false),
                new Column("AA", STRING, false),
        };

        Arrays.sort(cols, Columns.COLUMN_COMPARATOR);

        assertEquals("A", cols[0].name());
        assertEquals("B", cols[1].name());
        assertEquals("C", cols[2].name());
        assertEquals("AA", cols[3].name());
        assertEquals("AD", cols[4].name());
    }
}
