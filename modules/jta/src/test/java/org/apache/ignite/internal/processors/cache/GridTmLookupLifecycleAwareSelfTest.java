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
import org.apache.ignite.cache.jta.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.transaction.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test for {@link LifecycleAware} support for {@link CacheTmLookup}.
 */
public class GridTmLookupLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private boolean near;

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTxLookup extends GridAbstractLifecycleAwareSelfTest.TestLifecycleAware
        implements CacheTmLookup {
        /**
         */
        public TestTxLookup() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public TransactionManager getTm() {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        ccfg.setName(CACHE_NAME);

        ccfg.setTransactionManagerLookupClassName(TestTxLookup.class.getName());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterGridStart(Ignite ignite) {
        TestTxLookup tmLookup =
            (TestTxLookup)((IgniteKernal) ignite).context().cache().internalCache(CACHE_NAME).context().jta().tmLookup();

        assertNotNull(tmLookup);

        lifecycleAwares.add(tmLookup);
    }

    /** {@inheritDoc} */
    @Override public void testLifecycleAware() throws Exception {
        for (boolean nearEnabled : new boolean[] {true, false}) {
            near = nearEnabled;

            super.testLifecycleAware();
        }
    }
}
