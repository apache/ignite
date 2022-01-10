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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Test SQL data types.
 */
public class ItDataTypesTest extends AbstractBasicIntegrationTest {
    /**
     * Before all.
     */
    @Test
    public void testUnicodeStrings() {
        sql("CREATE TABLE string_table(key int primary key, val varchar)");

        String[] values = new String[]{"Кирилл", "Müller", "我是谁", "ASCII"};

        int key = 0;

        // Insert as inlined values.
        for (String val : values) {
            sql("INSERT INTO string_table (key, val) VALUES (?, ?)", key++, val);
        }

        List<List<?>> rows = sql("SELECT val FROM string_table");

        assertEquals(Set.of(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        sql("DELETE FROM string_table");

        // Insert as parameters.
        for (String val : values) {
            sql("INSERT INTO string_table (key, val) VALUES (?, ?)", key++, val);
        }

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

    /** Tests NOT NULL and DEFAULT column constraints. */
    @Test
    public void testCheckDefaultsAndNullables() {
        sql("CREATE TABLE tbl(c1 int primary key, c2 int NOT NULL, c3 int NOT NULL DEFAULT 100)");

        sql("INSERT INTO tbl(c1, c2) VALUES (1, 2)");

        List<List<?>> rows = sql("SELECT c3 FROM tbl");

        assertEquals(Set.of(100), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        sql("ALTER TABLE tbl ADD COLUMN c4 int NOT NULL DEFAULT 101");

        rows = sql("SELECT c4 FROM tbl");

        assertEquals(Set.of(101), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        //todo: correct exception https://issues.apache.org/jira/browse/IGNITE-16095
        assertThrows(IgniteException.class, () -> sql("INSERT INTO tbl(c1, c2) VALUES (2, NULL)"));
    }
}
