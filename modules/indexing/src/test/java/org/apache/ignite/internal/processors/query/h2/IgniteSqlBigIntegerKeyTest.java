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

package org.apache.ignite.internal.processors.query.h2;

import java.math.BigInteger;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Ensures that BigInteger can be used as key
 */
public class IgniteSqlBigIntegerKeyTest extends AbstractIndexingCommonTest {
    /** */
    private static final String CACHE_NAME = "Mycache";

    /** */
    private static final String SELECT_BY_KEY_SQL = "select _val from STUDENT where _key=?";

    /** */
    private static final String SELECT_BY_ID_SQL = "select _val from STUDENT where id=?";

    /** */
    private static final Student VALUE_OBJ = new Student(BigInteger.valueOf(42), "John Doe");

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server");

        Ignite cli = startClientGrid("client");

        IgniteCache<Object, Object> cache = cli.getOrCreateCache(new CacheConfiguration<>()
            .setIndexedTypes(BigInteger.class, Student.class)
            .setName(CACHE_NAME));

        cache.put(VALUE_OBJ.id, VALUE_OBJ);

    }

    /** */
    private IgniteCache<Object, Object> getCache() {
        return grid("client").cache(CACHE_NAME);
    }

    /** */
    @Test
    public void testBigIntegerKeyGet() {
        IgniteCache<Object, Object> cache = getCache();

        Object val = cache.get(BigInteger.valueOf(42));

        assertNotNull(val);
        assertTrue(val instanceof Student);
        assertEquals(VALUE_OBJ, val);

        val = cache.get(new BigInteger("42"));

        assertNotNull(val);
        assertTrue(val instanceof Student);
        assertEquals(VALUE_OBJ, val);
    }

    /** */
    @Test
    public void testBigIntegerKeyQuery() {
        IgniteCache<Object, Object> cache = getCache();

        checkQuery(cache, SELECT_BY_KEY_SQL, BigInteger.valueOf(42));
        checkQuery(cache, SELECT_BY_KEY_SQL, new BigInteger("42"));
    }

    /** */
    @Test
    public void testBigIntegerFieldQuery() {
        IgniteCache<Object, Object> cache = getCache();

        checkQuery(cache, SELECT_BY_ID_SQL, BigInteger.valueOf(42));
        checkQuery(cache, SELECT_BY_ID_SQL, new BigInteger("42"));
    }

    /** */
    private void checkQuery(IgniteCache<Object, Object> cache, String sql, Object arg) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(arg);

        QueryCursor<List<?>> query = cache.query(qry);

        List<List<?>> res = query.getAll();

        assertEquals(1, res.size());

        Object val = res.get(0).get(0);

        assertNotNull(val);
        assertTrue(val instanceof Student);
        assertEquals(VALUE_OBJ, val);
    }

    /**
     *
     */
    public static class Student {
        /** */
        @QuerySqlField(index = true)
        BigInteger id;

        /** */
        @QuerySqlField
        String name;

        /**
         * Constructor.
         */
        Student(BigInteger id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Student student = (Student)o;

            if (!id.equals(student.id))
                return false;
            return name.equals(student.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }
    }
}
