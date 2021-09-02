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

import javax.cache.configuration.Factory;
import javax.transaction.TransactionManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridAbstractLifecycleAwareSelfTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for {@link LifecycleAware} support for {@link CacheTmLookup}.
 */
public class GridJtaLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private boolean near;

    /** */
    private TmConfigurationType tmConfigurationType;

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTxLookup extends GridAbstractLifecycleAwareSelfTest.TestLifecycleAware
        implements CacheTmLookup {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void start() {
            super.start();

            assertNotNull(ignite);
        }

        /** {@inheritDoc} */
        @Nullable @Override public TransactionManager getTm() {
            return null;
        }
    }

    /**
     *
     */
    public static class TestTxFactory extends GridAbstractLifecycleAwareSelfTest.TestLifecycleAware
        implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void start() {
            super.start();

            assertNotNull(ignite);
        }

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
            return null;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        ccfg.setName(CACHE_NAME);

        switch (tmConfigurationType) {
            case CACHE_LOOKUP:
                ccfg.setTransactionManagerLookupClassName(TestTxLookup.class.getName());
                break;
            case GLOBAL_LOOKUP:
                cfg.getTransactionConfiguration().setTxManagerLookupClassName(TestTxLookup.class.getName());
                break;
            case FACTORY:
                cfg.getTransactionConfiguration().setTxManagerFactory(new TestTxFactory());
                break;
        }

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("OverlyStrongTypeCast")
    @Override protected void afterGridStart(Ignite ignite) {
        TestTxLookup tmLookup =
            (TestTxLookup)((IgniteKernal) ignite).context().cache().internalCache(CACHE_NAME).context().jta().tmLookup();

        assertNotNull(tmLookup);

        lifecycleAwares.add(tmLookup);
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLifecycleAware() throws Exception {
        // No-op, see anothre tests.
    }

    /** */
    @Test
    public void testCacheLookupLifecycleAware() throws Exception {
        tmConfigurationType = TmConfigurationType.CACHE_LOOKUP;

        checkLifecycleAware();
    }

    /** */
    @Test
    public void testGlobalLookupLifecycleAware() throws Exception {
        tmConfigurationType = TmConfigurationType.GLOBAL_LOOKUP;

        checkLifecycleAware();
    }

    /** */
    @Test
    public void testFactoryLifecycleAware() throws Exception {
        tmConfigurationType = TmConfigurationType.FACTORY;

        checkLifecycleAware();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkLifecycleAware() throws Exception {
        for (boolean nearEnabled : new boolean[] {true, false}) {
            near = nearEnabled;

            testLifecycleAware();
        }
    }

    /**
     *
     */
    private enum TmConfigurationType {
        /** */
        CACHE_LOOKUP,

        /** */
        GLOBAL_LOOKUP,

        /** */
        FACTORY
    }
}
