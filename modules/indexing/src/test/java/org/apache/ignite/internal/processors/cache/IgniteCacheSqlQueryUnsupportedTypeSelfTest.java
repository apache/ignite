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

package org.apache.ignite.internal.processors.cache;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks that both types can be inserted and queried:
 * 1) not (yet) supported by Ignite (even though they are supported by H2) : Instant,
 * 2) native sql types : Long, Integer, String...
 */
public class IgniteCacheSqlQueryUnsupportedTypeSelfTest extends GridCommonAbstractTest {
    /**
     * Name of the value type. Fake. We use binary objects to put to the cache.
     */
    private final String TYPE_NAME = "SomeType";

    /**
     * Starts cluster with one node.
     */
    @Before
    public void setupCluster() throws Exception {
        startGrids(1);
    }

    /**
     * Stops the cluster.
     */
    @After
    public void tearOff() {
        stopAllGrids();
    }

    /**
     * Create cache configuration containing table that has field with the specified type.
     *
     * @param testedFldCls type of the "testFld" column/field.
     * @return cache configuration.
     */
    private CacheConfiguration cacheConfigForType(Class testedFldCls) {
        return new CacheConfiguration()
            .setName("CACHE_" + testedFldCls.getSimpleName())
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class.getName(), TYPE_NAME)
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("testFld", testedFldCls.getName(), null)
                    .setTableName("PERSON")
                    .setIndexes(Arrays.asList(
                        new QueryIndex("id", true),
                        new QueryIndex("testFld", true)
                    ))
            ));
    }

    /**
     * Check that both natively supported and unsupported by IgniteSQL types are correctly inserted and queried.
     */
    @Test
    public void testUnsupportedSqlType() {
        Object[] testedValues = {
            // Types that maps on sql types natively:
            "String",
            42,
            7L,
            true,
            1.2d,
            (byte)25,
            (short)54,
            //Character is unsupported
            3.4f,
            "garbage".getBytes(),
            UUID.randomUUID(),
            new BigDecimal("1.450"),
            new java.sql.Date(System.currentTimeMillis()),
            new java.sql.Time(System.currentTimeMillis()),
            new java.sql.Timestamp(System.currentTimeMillis()),
            new java.util.Date(),
            // No tests for the geometry
            java.time.LocalDate.now(),
            java.time.LocalTime.now(),
            java.time.LocalDateTime.now(),

            // Non native sql types (mapped to JAVA_OBJECT):
            Instant.now(),
            new Object(),
            new HashMap<>()
        };

        for (Object val : testedValues)
            testType(val, val.getClass());

        testType(null, Void.class);
        testType(null, Void.TYPE);
    }

    /**
     * Check that specified value can be inserted into column of specified type.
     * @param val value to be inserted.
     * @param testedType type of the column.
     */
    private void testType(Object val, Class testedType) {
        try (IgniteCache<Integer, BinaryObject> cache = grid(0).createCache(cacheConfigForType(testedType)).withKeepBinary()) {
            cache.put(1, createVal(1, val));

            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM PERSON WHERE testFld = (select testFld from person where id = 1)")).getAll();

            assertEquals(res.get(0).get(0), 1);
            assertEquals(res.get(0).get(1), val);
        }
        catch (Exception e) {
            throw new AssertionError("Couldn't validate field of type " + testedType +
                " with the sample value " + val, e);
        }
    }

    @Test
    public void test() {
        createVal(1, 42);
    }

    /**
     * Create binary object with field of tested type. This object matches table, created by {@linkplain #cacheConfigForType(Class)}.
     *
     * @param id id field value.
     * @param testedFldVal value of the tested field.
     * @return binary object to put into the cache.
     */
    private BinaryObject createVal(int id, Object testedFldVal) {
        return grid(0).binary().builder(TYPE_NAME)
            .setField("id", id)
            .setField("testFld", testedFldVal, ((Class<? super Object>)testedFldVal.getClass()))
            .build();
    }
}
