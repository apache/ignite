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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for query entity validation.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class QueryEntityValidationSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test null value type.
     *
     * @throws Exception If failed.
     */
    public void testValueTypeNull() throws Exception {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");

        assertValidationThrows(entity, "Value type cannot be null or empty");
    }

    /**
     * Test failure if index type is null.
     */
    public void testIndexTypeNull() throws Exception {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");
        entity.setValueType("Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("a", Integer.class.getName());

        entity.setFields(fields);

        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();

        idxFields.put("a", true);

        QueryIndex idx = new QueryIndex().setName("idx").setFields(idxFields).setIndexType(null);

        List<QueryIndex> idxs = new ArrayList<>();

        idxs.add(idx);

        entity.setIndexes(idxs);

        assertValidationThrows(entity, "Index type is not set");
    }

    /**
     * Test duplicated index name.
     */
    public void testIndexNameDuplicate() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");
        entity.setValueType("Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("a", Integer.class.getName());
        fields.put("b", Integer.class.getName());

        entity.setFields(fields);

        LinkedHashMap<String, Boolean> idx1Fields = new LinkedHashMap<>();
        LinkedHashMap<String, Boolean> idx2Fields = new LinkedHashMap<>();

        idx1Fields.put("a", true);
        idx1Fields.put("b", true);

        QueryIndex idx1 = new QueryIndex().setName("idx").setFields(idx1Fields);
        QueryIndex idx2 = new QueryIndex().setName("idx").setFields(idx2Fields);

        List<QueryIndex> idxs = new ArrayList<>();

        idxs.add(idx1);
        idxs.add(idx2);

        entity.setIndexes(idxs);

        assertValidationThrows(entity, "Duplicate index name");
    }

    /**
     * Test multiple key columns with JDK key type.
     */
    public void testMultipleColumnsJdkKey() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType("Value");

        entity.setFields(fields("mulKeyCol1", "Type1", "mulKeyCol2", "Type2"));

        entity.setKeyFields(new HashSet<>(F.asList("mulKeyCol1", "mulKeyCol2")));

        assertValidationThrows(entity, "Key type may not point at JDK type when multiple key columns are defined.");
    }

    /**
     * Test multiple value columns with JDK value type.
     */
    public void testMultipleColumnsJdkValue() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");
        entity.setValueType(Integer.class.getName());

        entity.setFields(fields("mulValCol1", "Type1", "mulValCol2", "Type2"));

        assertValidationThrows(entity, "Value type may not point at JDK type when multiple value columns are defined.");
    }

    /**
     * Test that type of specified key column matches that of query entity key.
     */
    public void testJdkKeyColumnTypeMismatch() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType("Value");

        entity.setKeyFieldName("sngKeyCol");

        entity.setFields(fields("sngKeyCol", String.class.getName()));

        assertValidationThrows(entity, "Explicitly specified key column sngKeyCol points at a type different " +
            "from query entity SQL key type [entityKeyType=java.lang.Integer,keyColumnType=java.lang.String]");
    }

    /**
     * Test that type of specified key column matches that of query entity key.
     */
    public void testJdkValueColumnTypeMismatch() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");
        entity.setValueType(Integer.class.getName());

        entity.setValueFieldName("sngValCol");

        entity.setFields(fields("sngValCol", String.class.getName()));

        assertValidationThrows(entity, "Explicitly specified value column sngValCol points at a type different " +
            "from query entity SQL value type [entityValueType=java.lang.Integer,valueColumnType=java.lang.String]");
    }

    /**
     * @param e Entity to validate.
     * @param msg Expected exception message.
     */
    private void assertValidationThrows(final QueryEntity e, String msg) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

                ccfg.setQueryEntities(Collections.singleton(e));

                grid(0).createCache(ccfg);

                return null;
            }
        }, IgniteCheckedException.class, msg);
    }

    /**
     * @param flds Fields.
     * @return Fields map.
     */
    private static LinkedHashMap<String, String> fields(String... flds) {
        assertTrue(flds.length % 2 == 0);

        LinkedHashMap<String, String> res = new LinkedHashMap<>();

        for (int i = 0; i < flds.length; i += 2)
            res.put(flds[i], flds[i + 1]);

        return res;
    }
}
