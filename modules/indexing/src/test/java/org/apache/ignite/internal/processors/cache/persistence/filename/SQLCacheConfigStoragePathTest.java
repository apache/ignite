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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;

import static org.apache.ignite.internal.cdc.SqlCdcTest.executeSql;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;

/** */
public class SQLCacheConfigStoragePathTest extends CacheConfigStoragePathTest {
    /** {@inheritDoc} */
    @Override CacheConfiguration[] ccfgs() {
        return new CacheConfiguration[] {
            ccfg("cache0", null).setSqlSchema(DFLT_SCHEMA).setQueryEntities(Collections.singletonList(new QueryEntity()
                .setTableName("T0")
                .setKeyFieldName("ID")
                .setKeyType(Long.class.getName())
                .setValueType("Cache0Val")
                .setFields(new LinkedHashMap<>(Map.of("ID", Long.class.getName(), "NAME", String.class.getName())))
                .setIndexes(List.of(new QueryIndex("NAME"))))),

            ccfg("cache1", "grp1").setSqlSchema(DFLT_SCHEMA).setQueryEntities(Collections.singletonList(new QueryEntity()
                .setTableName("T1")
                .setKeyFieldName("ID")
                .setKeyType(Long.class.getName())
                .setValueType("Cache1Val")
                .setFields(new LinkedHashMap<>(Map.of(
                    "ID", Long.class.getName(),
                    "DEP", String.class.getName(),
                    "DATE", Date.class.getName())))
                .setIndexes(List.of(new QueryIndex("DEP"))))),
        };
    }

    /** {@inheritDoc} */
    @Override void putData() {
        forAllEntries((c, i) -> {
            if (c.getName().equals("cache0"))
                executeSql(grid(0), "INSERT INTO T0(ID, NAME) VALUES(?, ?)", i, name(i));
            else if (c.getName().equals("cache1"))
                executeSql(grid(0), "INSERT INTO T1(ID, DEP, DATE) VALUES(?, ?, ?)", i, dep(i), new Date());
            else
                throw new IllegalStateException("Unknown cache: " + c.getName());
        });
    }

    /** {@inheritDoc} */
    @Override void checkDataExists() {
        forAllEntries((c, i) -> {
            if (c.getName().equals("cache0")) {
                List<List<?>> rows = executeSql(grid(0), "SELECT NAME FROM T0 WHERE ID = ?", i);

                assertEquals(1, rows.size());
                assertEquals(name(i), rows.get(0).get(0));
            }
            else if (c.getName().equals("cache1")) {
                List<List<?>> rows = executeSql(grid(0), "SELECT DEP, DATE FROM T1 WHERE ID = ?", i);

                assertEquals(1, rows.size());
                assertEquals(dep(i), rows.get(0).get(0));
                assertTrue(rows.get(0).get(1) instanceof Date);
            }
            else
                throw new IllegalStateException("Unknown cache: " + c.getName());
        });

    }

    /** */
    private static String dep(int i) {
        return i + " dep";
    }

    /** */
    private static String name(int i) {
        return i + " name";
    }
}
