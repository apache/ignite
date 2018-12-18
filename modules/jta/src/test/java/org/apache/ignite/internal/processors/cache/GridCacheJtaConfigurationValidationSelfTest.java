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

import java.util.concurrent.Callable;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Configuration validation test.
 */
@RunWith(JUnit4.class)
public class GridCacheJtaConfigurationValidationSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(ATOMIC);

        ccfg.setTransactionManagerLookupClassName(TestTxLookup.class.getName());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests that a user did not set 'transactionManagerLookupClassName' property for atomic cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicWithTmLookup() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTxLookup implements CacheTmLookup {
        /** {@inheritDoc} */
        @Nullable @Override public TransactionManager getTm() {
            return null;
        }
    }
}
