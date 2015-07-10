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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.config.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.net.*;
import java.util.*;

/**
 *
 */
public class IgniteCacheQueryNoServerClassesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** */
    private static final int CLIENT_IDX = NODES_CNT - 1;

    /** */
    private static final String ANNOTATED_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.IndexAnnotatedValue";

    /** */
    private static final String PLAIN_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.IndexValue";

    /** */
    private static ClassLoader ldr;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ldr = new GridTestExternalClassLoader(new URL[]{
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        ldr = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        Ignition.setDefaultClassLoader(ldr);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(CLIENT_IDX).equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryAnnotatedClass() throws Exception {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        Class<?> valCls = ldr.loadClass(ANNOTATED_CLASS_NAME);

        Thread.currentThread().setContextClassLoader(ldr);

        cfg.setIndexedTypes(Integer.class, valCls);

        IgniteCache<Object, Object> clientCache = ignite(CLIENT_IDX).createCache(cfg);

        try {
            for (int i = 0; i < 100; i++)
                clientCache.put(i, value(valCls, i, "value" + i, new Date(i * 1000), "otherValue" + i));

            // Check SQL query.
            List<Cache.Entry<Object, Object>> res = clientCache.query(new SqlQuery<>(valCls, "field1 >= 50")).getAll();

            assertEquals(50, res.size());

            // Check SQL fields query.
            List<List<?>> rows = clientCache.query(new SqlFieldsQuery("select field1, field2 from IndexAnnotatedValue " +
                "where field1 >= 50")).getAll();

            assertEquals(50, rows.size());
        }
        finally {
            ignite(CLIENT_IDX).destroyCache(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryMetadata() throws Exception {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        Class<?> valCls = ldr.loadClass(PLAIN_CLASS_NAME);

        Thread.currentThread().setContextClassLoader(ldr);

        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(Integer.class);
        meta.setValueType(valCls);

        meta.setAscendingFields(F.asMap("field1", (Class<?>)Integer.class, "field2", String.class, "field3", Date.class));
        meta.setQueryFields(F.<String, Class<?>>asMap("field4", String.class));

        cfg.setTypeMetadata(F.asList(meta));

        IgniteCache<Object, Object> clientCache = ignite(CLIENT_IDX).createCache(cfg);

        try {
            for (int i = 0; i < 100; i++)
                clientCache.put(i, value(valCls, i, "value" + i, new Date(i * 1000), "otherValue" + i));

            // Check SQL query.
            List<Cache.Entry<Object, Object>> res = clientCache.query(new SqlQuery<>(valCls, "field1 >= 50")).getAll();

            assertEquals(50, res.size());

            // Check SQL fields query.
            List<List<?>> rows = clientCache.query(new SqlFieldsQuery("select field1, field2 from IndexValue " +
                "where field1 >= 50")).getAll();

            assertEquals(50, rows.size());
        }
        finally {
            ignite(CLIENT_IDX).destroyCache(null);
        }
    }

    /**
     * @param cls Class to instantiate.
     * @param field1 Field 1 value.
     * @param field2 Field 2 value.
     * @param field3 Field 3 value.
     * @param field4 Field 4 value.
     * @return Filled object.
     * @throws Exception If error occurred.
     */
    private Object value(Class<?> cls, int field1, String field2, Date field3, Object field4) throws Exception {
        Object res = cls.newInstance();

        GridTestUtils.setFieldValue(res, "field1", field1);
        GridTestUtils.setFieldValue(res, "field2", field2);
        GridTestUtils.setFieldValue(res, "field3", field3);
        GridTestUtils.setFieldValue(res, "field4", field4);

        return res;
    }
}
