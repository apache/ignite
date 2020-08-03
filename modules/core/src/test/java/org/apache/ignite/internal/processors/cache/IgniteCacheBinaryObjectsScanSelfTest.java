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

import java.util.ArrayList;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheBinaryObjectsScanSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CLS_NAME = "org.apache.ignite.tests.p2p.cache.Person";

    /** */
    private static final String PERSON_KEY_CLS_NAME = "org.apache.ignite.tests.p2p.cache.PersonKey";

    /** */
    private static ClassLoader ldr;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ldr = getExternalClassLoader();

        startGrids(3);

        startClientGrid("client");

        populateCache(ldr);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ldr = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(getIncludeEventTypes());

        cfg.setMarshaller(null);
        cfg.setPeerClassLoadingEnabled(false);

        if ("client".equals(igniteInstanceName))
            cfg.setClassLoader(ldr);

        return cfg;
    }

    /**
     * @return EventTypes to record.
     */
    protected int[] getIncludeEventTypes() {
        return new int[0];
    }

    /**
     * @param ldr Class loader.
     * @throws Exception If failed.
     */
    private void populateCache(ClassLoader ldr) throws Exception {
        Class<?> keyCls = ldr.loadClass(PERSON_KEY_CLS_NAME);
        Class<?> cls = ldr.loadClass(PERSON_CLS_NAME);

        Ignite client = grid("client");

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>("testCache");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(cfg);

        for (int i = 0; i < 100; i++) {
            Object key = keyCls.newInstance();

            GridTestUtils.setFieldValue(key, "id", i);

            cache.put(key, cls.newInstance());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanNoClasses() throws Exception {
        Ignite client = grid("client");

        IgniteCache<Object, Object> cache = client.cache("testCache");

        List<Cache.Entry<Object, Object>> entries = cache.query(new ScanQuery<>()).getAll();

        assertEquals(100, entries.size());

        for (Cache.Entry<Object, Object> entry : entries) {
            assertEquals(PERSON_KEY_CLS_NAME, entry.getKey().getClass().getName());
            assertEquals(PERSON_CLS_NAME, entry.getValue().getClass().getName());
        }

        entries = new ArrayList<>();

        int partCnt = client.affinity("testCache").partitions();

        for (int i = 0; i < partCnt; i++)
            entries.addAll(cache.query(new ScanQuery<>(i)).getAll());

        assertEquals(100, entries.size());

        for (Cache.Entry<Object, Object> entry : entries) {
            assertEquals(PERSON_KEY_CLS_NAME, entry.getKey().getClass().getName());
            assertEquals(PERSON_CLS_NAME, entry.getValue().getClass().getName());
        }
    }
}
