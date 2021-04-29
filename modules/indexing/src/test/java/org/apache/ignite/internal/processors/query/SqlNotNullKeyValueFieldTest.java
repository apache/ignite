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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests key and value fields with not-null constraints.
 */
@RunWith(Parameterized.class)
public class SqlNotNullKeyValueFieldTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public String keyName;

    /** */
    @Parameterized.Parameter(1)
    public String valName;

    /** */
    @Parameterized.Parameters(name = "key={0} val={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
                {"id", "val"},
                {"ID", "VAL"}
        });
    }

    /** */
    @Before
    public void setup() throws Exception {
        startGrid(0);

        startClientGrid("client");
    }

    /** */
    @After
    public void clean() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testQueryEntity() {
        IgniteCache<?, ?> cache = grid("client").createCache(
            new CacheConfiguration<>("default").setQueryEntities(
                Collections.singleton(new QueryEntity()
                    .setTableName("Person")
                    .setKeyFieldName(keyName)
                    .setKeyType(Integer.class.getName())
                    .setValueFieldName(valName)
                    .setValueType(String.class.getName())
                    .setFields(new LinkedHashMap<>(
                            F.asMap(keyName, Integer.class.getName(),
                                    valName, String.class.getName())))
                    .setNotNullFields(F.asSet(keyName, valName)))));

        String qry = "insert into Person (" + keyName + ", " + valName + ") values (1, 'John Doe')";

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(qry));

        assertEquals(1, ((Number) cursor.getAll().get(0).get(0)).intValue());
    }

    /** */
    @Test
    public void testQuotedDDL() {
        doTestDDL("\"" + keyName + "\"", "\"" + valName + "\"");
    }

    /** */
    @Test
    public void testUnquotedDDL() {
        doTestDDL(keyName, valName);
    }

    /** */
    private void doTestDDL(String key, String val) {
        IgniteCache<?, ?> cache = grid("client").createCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE Person(" + key +
                " INTEGER PRIMARY KEY NOT NULL, " + val + " VARCHAR NOT NULL)")).getAll();

        String qry = "insert into Person (" + key + ", " + val + ") values (1, 'John Doe')";

        QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(qry));

        assertEquals(1, ((Number) cursor.getAll().get(0).get(0)).intValue());
    }
}
