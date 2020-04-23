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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ContinuousQueryRemoteFilterMissingInClassPathSelfTest extends GridCommonAbstractTest {
    /** URL of classes. */
    private static final URL[] URLS;

    /** */
    public static final String EXT_FILTER_CLASS = "org.apache.ignite.tests.p2p.CacheDeploymentCacheEntryEventSerializableFilter";

    static {
        try {
            URLS = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** */
    private IgniteLogger log;

    /** */
    private boolean setExternalLoader;

    /** */
    private final ClassLoader extLdr = new URLClassLoader(URLS, getClass().getClassLoader());

    /** */
    private boolean setFilterAttr;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cacheCfg.setNodeFilter(node -> node.attribute("filter") == null);

        cfg.setCacheConfiguration(cacheCfg);

        if (setExternalLoader)
            cfg.setClassLoader(extLdr);

        if (log != null)
            cfg.setGridLogger(log);

        if (setFilterAttr)
            cfg.setUserAttributes(U.map("filter", 1));

        return cfg;
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testClientJoinsMissingClassWarning() throws Exception {
        setExternalLoader = true;
        Ignite ignite0 = startGrid(1);

        executeContinuousQuery(ignite0.cache(DEFAULT_CACHE_NAME));

        log = new GridStringLogger();
        setExternalLoader = false;

        startClientGrid(2);

        String logStr = log.toString();

        assertTrue(logStr.contains("Failed to unmarshal continuous query remote filter on client node. " +
            "Can be ignored.") || logStr.contains("Failed to unmarshal continuous routine handler"));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testClientJoinsExtClassLoaderNoWarning() throws Exception {
        setExternalLoader = true;

        Ignite ignite0 = startGrid(1);

        executeContinuousQuery(ignite0.cache(DEFAULT_CACHE_NAME));

        log = new GridStringLogger();

        startClientGrid(2);

        assertTrue(!log.toString().contains("Failed to unmarshal continuous query remote filter on client node. " +
            "Can be ignored."));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testServerJoinsMissingClassException() throws Exception {
        setExternalLoader = true;
        Ignite ignite0 = startGrid(1);

        executeContinuousQuery(ignite0.cache(DEFAULT_CACHE_NAME));

        ListeningTestLogger listeningLogger = new ListeningTestLogger();

        log = listeningLogger;

        LogListener lsnr = LogListener.matches(logStr ->
            logStr.contains("class org.apache.ignite.IgniteCheckedException: " +
                "Failed to find class with given class loader for unmarshalling")
                || logStr.contains("Failed to unmarshal continuous routine handler"
            )).build();

        listeningLogger.registerListener(lsnr);

        setExternalLoader = false;

        GridTestUtils.assertThrows(log, () -> startGrid(2), IgniteCheckedException.class, "Failed to start");

        assertTrue(lsnr.check());
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testServerJoinsExtClassLoaderNoException() throws Exception {
        setExternalLoader = true;
        Ignite ignite0 = startGrid(1);

        executeContinuousQuery(ignite0.cache(DEFAULT_CACHE_NAME));

        log = new GridStringLogger();

        startGrid(2);

        assertTrue(!log.toString().contains("class org.apache.ignite.IgniteCheckedException: " +
            "Failed to find class with given class loader for unmarshalling"));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testServerMissingClassFailsRegistration() throws Exception {
        setExternalLoader = true;

        Ignite ign1 = startGrid(1);

        setExternalLoader = false;

        startGrid(2);

        try {
            executeContinuousQuery(ign1.cache(DEFAULT_CACHE_NAME));

            fail("Exception is expected");
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, EXT_FILTER_CLASS, ClassNotFoundException.class));
        }
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testClientMissingClassDoesNotFailRegistration() throws Exception {
        setExternalLoader = true;

        Ignite ign1 = startGrid(1);

        setExternalLoader = false;

        startClientGrid(2);

        executeContinuousQuery(ign1.cache(DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testNodeFilterServerMissingClassDoesNotFailRegistration() throws Exception {
        setExternalLoader = true;

        Ignite ign1 = startGrid(1);

        setExternalLoader = false;

        setFilterAttr = true;

        startGrid(2);

        executeContinuousQuery(ign1.cache(DEFAULT_CACHE_NAME));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If fail.
     */
    private void executeContinuousQuery(IgniteCache<Object, Object> cache) throws Exception {
        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

        qry.setLocalListener(
            new CacheEntryUpdatedListener<Integer, String>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> events)
                    throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends Integer, ? extends String> event : events)
                        System.out.println("Key = " + event.getKey() + ", Value = " + event.getValue());
                }
            }
        );

        Class<CacheEntryEventSerializableFilter> remoteFilterCls = (Class<CacheEntryEventSerializableFilter>)
            extLdr.loadClass(EXT_FILTER_CLASS);

        qry.setRemoteFilterFactory(new ClassFilterFactory(remoteFilterCls));

        cache.query(qry);

        for (int i = 0; i < 100; i++)
            cache.put(i, "Message " + i);
    }

    /**
     *
     */
    private static class ClassFilterFactory implements Factory<CacheEntryEventFilter<Integer, String>> {
        /** */
        private Class<CacheEntryEventSerializableFilter> cls;

        /**
         * @param cls Class.
         */
        public ClassFilterFactory(Class<CacheEntryEventSerializableFilter> cls) {
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public CacheEntryEventSerializableFilter<Integer, String> create() {
            try {
                return (CacheEntryEventSerializableFilter<Integer, String>)cls.newInstance();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
