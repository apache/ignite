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

package org.apache.ignite.internal.calcite;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test SQL data types.
 */
public class ITDataTypesTest extends AbstractBasicIntegrationTest {
    /** */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15107")
    @Test
    public void testUnicodeStrings() {
        sql("CREATE TABLE string_table(key int primary key, val varchar)");

        String[] values = new String[] {"Кирилл", "Müller", "我是谁", "ASCII"};

        int key = 0;

        // Insert as inlined values.
        for (String val : values)
            sql("INSERT INTO string_table (key, val) VALUES (?, ?)", key++, val);

        List<List<?>> rows = sql("SELECT val FROM string_table");

        assertEquals(Set.of(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        sql("DELETE FROM string_table");

        // Insert as parameters.
        for (String val : values)
            sql("INSERT INTO string_table (key, val) VALUES (?, ?)", key++, val);

        rows = sql("SELECT val FROM string_table");

        assertEquals(Set.of(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        rows = sql("SELECT substring(val, 1, 2) FROM string_table");

        assertEquals(Set.of("Ки", "Mü", "我是", "AS"),
            rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        for (String val : values) {
            rows = sql("SELECT char_length(val) FROM string_table WHERE val = ?", val);

            assertEquals(1, rows.size());
            assertEquals(val.length(), rows.get(0).get(0));
        }
    }
}
