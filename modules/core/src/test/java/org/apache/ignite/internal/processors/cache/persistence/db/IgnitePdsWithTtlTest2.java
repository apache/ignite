/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

public class IgnitePdsWithTtlTest2 extends GridCommonAbstractTest {
    /** */
    public static AtomicBoolean handleFired = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public CacheConfiguration getCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);

        ccfg.setAtomicityMode(ATOMIC);

        ccfg.setBackups(1);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32768));

        ccfg.setEagerTtl(true);

        ccfg.setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 20)));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(100L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ));

        cfg.setCacheConfiguration(
            getCacheConfiguration("cache_1"),
            getCacheConfiguration("cache_2"),
            getCacheConfiguration("cache_3"),
            getCacheConfiguration("cache_4"),
            getCacheConfiguration("cache_5"),
            getCacheConfiguration("cache_6"),
            getCacheConfiguration("cache_7"),
            getCacheConfiguration("cache_8"),
            getCacheConfiguration("cache_9"),
            getCacheConfiguration("cache_10"),
            getCacheConfiguration("cache_11"),
            getCacheConfiguration("cache_12"),
            getCacheConfiguration("cache_13"),
            getCacheConfiguration("cache_14"),
            getCacheConfiguration("cache_15"),
            getCacheConfiguration("cache_16"),
            getCacheConfiguration("cache_17"),
            getCacheConfiguration("cache_18"),
            getCacheConfiguration("cache_19"),
            getCacheConfiguration("cache_20")
        );

        cfg.setFailureHandler(new CustomStopNodeOrHaltFailureHandler());

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsAppliedToManyCaches() throws Exception {
        handleFired.set(false);

        startGrid(0);

        assertFalse(handleFired.get());
    }

    private class CustomStopNodeOrHaltFailureHandler extends NoOpFailureHandler {
        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            boolean res = super.handle(ignite, failureCtx);

            handleFired.set(true);

            return res;
        }
    }
}
