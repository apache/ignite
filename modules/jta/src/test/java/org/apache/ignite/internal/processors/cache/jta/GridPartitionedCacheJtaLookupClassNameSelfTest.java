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

package org.apache.ignite.internal.processors.cache.jta;

import java.util.concurrent.Callable;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Lookup class name based JTA integration test using PARTITIONED cache.
 */
public class GridPartitionedCacheJtaLookupClassNameSelfTest extends AbstractCacheJtaSelfTest {
    /** {@inheritDoc} */
    @Override protected void configureJta(IgniteConfiguration cfg) {
        cfg.getTransactionConfiguration().setTxManagerLookupClassName(TestTmLookup.class.getName());
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10723")
    @Test
    public void testIncompatibleTmLookup() {
        final IgniteEx ignite = grid(0);

        final CacheConfiguration cacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cacheCfg.setName("Foo");
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setTransactionManagerLookupClassName(TestTmLookup2.class.getName());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws IgniteException {
                ignite.createCache(cacheCfg);

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTmLookup implements CacheTmLookup {
        /** {@inheritDoc} */
        @Override public TransactionManager getTm() {
            return jotm.getTransactionManager();
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTmLookup2 implements CacheTmLookup {
        /** {@inheritDoc} */
        @Override public TransactionManager getTm() {
            return null;
        }
    }
}
