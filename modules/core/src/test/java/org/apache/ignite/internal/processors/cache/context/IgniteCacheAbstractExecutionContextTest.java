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

package org.apache.ignite.internal.processors.cache.context;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.config.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.net.*;
import java.util.*;

/**
 *
 */
public abstract class IgniteCacheAbstractExecutionContextTest extends IgniteCacheAbstractTest {
    /**  */
    public static final int ITERATIONS = 1000;

    /** */
    public static final String EXPIRY_POLICY_CLASS = "org.apache.ignite.tests.p2p.CacheExpirePolicyNoop";

    /** */
    public static final String ENTRY_LISTENER_CLASS = "org.apache.ignite.tests.p2p.CacheEntryListenerConfiguration";

    /** */
    public static final String ENTRY_PROCESSOR_CLASS = "org.apache.ignite.tests.p2p.CacheEntryProcessorNoop";

    /** */
    public static final String CACHE_STORE_CLASS = "org.apache.ignite.tests.p2p.CacheStoreNoop";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClassLoader(new UsersClassLoader());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheStore<?, ?> cacheStore() {
        try {
            return loadClass(new UsersClassLoader(), CACHE_STORE_CLASS);
        }
        catch (Exception e){
            throw new CacheException(e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntryProcessor() throws Exception {
        UsersClassLoader classLdr = (UsersClassLoader)ignite(0).configuration().getClassLoader();

        IgniteCache<Object, Object> cache = ignite(0).jcache(null);

        Set<Integer> keys = new TreeSet<>();

        for (int i = 0; i < ITERATIONS; i++) {
            cache.put(i, i);

            keys.add(i);
        }

        Map<Object, EntryProcessorResult<Object>> res = cache.invokeAll(
            keys,
            this.<EntryProcessor<Object, Object, Object>>loadClass(classLdr, ENTRY_PROCESSOR_CLASS));

        assertEquals(ITERATIONS, res.size());

        for (EntryProcessorResult<Object> val : res.values())
            assertEquals(42, (long)val.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheEntryListener() throws Exception {
        UsersClassLoader classLdr = (UsersClassLoader)ignite(0).configuration().getClassLoader();

        IgniteCache<Object, Object> cache = ignite(0).jcache(null);

        CacheEntryListenerConfiguration<Object, Object> list = loadClass(ignite(0).configuration().getClassLoader(),
            ENTRY_LISTENER_CLASS);

        cache.registerCacheEntryListener(list);

        for (int i = ITERATIONS; i < 2 * ITERATIONS; i++)
            cache.put(i, i);

        assertEquals(ITERATIONS, U.invoke(list.getClass(), list, "getCnt", null));
        assertEquals(2, (int)classLdr.classUsedCnt(ENTRY_LISTENER_CLASS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpirePolicies() throws Exception {
        UsersClassLoader classLdr = (UsersClassLoader)ignite(0).configuration().getClassLoader();

        IgniteCache<Object, Object> cache = ignite(0).jcache(null);

        cache.withExpiryPolicy(this.<ExpiryPolicy>loadClass(classLdr, EXPIRY_POLICY_CLASS));

        assertEquals(2, (int)classLdr.classUsedCnt(EXPIRY_POLICY_CLASS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheLoaderWriter() throws Exception {
        IgniteCache<Object, Object> cache = ignite(0).jcache(null);

        UsersClassLoader classLdr = (UsersClassLoader)ignite(0).configuration().getClassLoader();

        assertEquals(42L, cache.get(99999999999999L));

        assertEquals(1, (int)classLdr.classUsedCnt(CACHE_STORE_CLASS));
    }

    /**
     * @return Loaded class.
     * @throws Exception Thrown if any exception occurs.
     */
    private <T> T loadClass(ClassLoader usersClassLdr, String className)
        throws Exception {
        assertNotNull(usersClassLdr);

        assertNotNull(usersClassLdr.loadClass(className));

        return (T)usersClassLdr.loadClass(className).newInstance();
    }

    /**
     *
     */
    private static class UsersClassLoader extends GridTestExternalClassLoader {
        /**
         *
         */
        private Map<String, Integer> loadedClasses = new HashMap<>();

        /**
         *
         * @throws MalformedURLException If failed
         */
        public UsersClassLoader() throws MalformedURLException {
            super(new URL[]{new URL(GridTestProperties.getProperty("p2p.uri.cls"))});
        }

        /**
         *
         */
        @Override public Class<?> loadClass(String name) throws ClassNotFoundException {
            int count = !loadedClasses.containsKey(name) ? 0 : loadedClasses.get(name);

            ++count;

            loadedClasses.put(name, count);

            return super.loadClass(name);
        }

        public Integer classUsedCnt(String name){
            return loadedClasses.get(name);
        }
    }
}
