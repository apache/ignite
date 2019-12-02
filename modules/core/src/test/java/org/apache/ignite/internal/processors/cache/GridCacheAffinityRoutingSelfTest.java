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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobFailoverException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Affinity routing tests.
 */
@RunWith(JUnit4.class)
public class GridCacheAffinityRoutingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final String NON_DFLT_CACHE_NAME = "myCache";

    /** */
    private static final int KEY_CNT = 50;

    /** */
    private static final int MAX_FAILOVER_ATTEMPTS = 5;

    /**
     * Constructs test.
     */
    public GridCacheAffinityRoutingSelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        AlwaysFailoverSpi failSpi = new AlwaysFailoverSpi();
        failSpi.setMaximumFailoverAttempts(MAX_FAILOVER_ATTEMPTS);
        cfg.setFailoverSpi(failSpi);

        if (!igniteInstanceName.equals(getTestIgniteInstanceName(GRID_CNT))) {
            // Default cache configuration.
            CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

            dfltCacheCfg.setCacheMode(PARTITIONED);
            dfltCacheCfg.setBackups(1);
            dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            // Non-default cache configuration.
            CacheConfiguration namedCacheCfg = defaultCacheConfiguration();

            namedCacheCfg.setCacheMode(PARTITIONED);
            namedCacheCfg.setBackups(1);
            namedCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            namedCacheCfg.setName(NON_DFLT_CACHE_NAME);

            cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg);
        }
        else {
            // No cache should be configured for extra node.
            cfg.setCacheConfiguration();
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        assert G.allGrids().size() == GRID_CNT;

        for (int i = 0; i < KEY_CNT; i++) {
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

            grid(0).cache(NON_DFLT_CACHE_NAME).put(i, i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityRun() throws Exception {
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, i, new CheckRunnable(i, i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCallRestartFails() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, "key",
                    new FailedCallable("key", MAX_FAILOVER_ATTEMPTS + 1));
                return null;
            }
        }, ClusterTopologyException.class, "Failed to failover a job to another node");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCallRestart() throws Exception {
        assertEquals(MAX_FAILOVER_ATTEMPTS,
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, "key",
                new FailedCallable("key", MAX_FAILOVER_ATTEMPTS)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityRunRestartFails() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, "key",
                    new FailedRunnable("key", MAX_FAILOVER_ATTEMPTS + 1));
                return null;
            }
        }, ClusterTopologyException.class, "Failed to failover a job to another node");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityRunRestart() throws Exception {
        grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, "key", new FailedRunnable("key", MAX_FAILOVER_ATTEMPTS));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityRunComplexKey() throws Exception {
        for (int i = 0; i < KEY_CNT; i++) {
            AffinityTestKey key = new AffinityTestKey(i);

            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, i, new CheckRunnable(i, key));
            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, key, new CheckRunnable(i, key));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCall() throws Exception {
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, i, new CheckCallable(i, i));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCallComplexKey() throws Exception {
        for (int i = 0; i < KEY_CNT; i++) {
            final AffinityTestKey key = new AffinityTestKey(i);

            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, i, new CheckCallable(i, key));
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, key, new CheckCallable(i, key));
        }
    }

    /**
     * Test key.
     */
    protected static class AffinityTestKey {
        /** Affinity key. */
        @AffinityKeyMapped
        private final int affKey;

        /**
         * @param affKey Affinity key.
         */
        private AffinityTestKey(int affKey) {
            this.affKey = affKey;
        }

        /**
         * @return Affinity key.
         */
        public int affinityKey() {
            return affKey;
        }
    }

    /**
     * Test runnable.
     */
    private static class CheckRunnable extends CAX {
        /** Affinity key. */
        private final Object affKey;

        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         * @param key Key.
         */
        private CheckRunnable(Object affKey, Object key) {
            this.affKey = affKey;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void applyx() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(affKey).id());
            assert ignite.cluster().localNode().id().equals(ignite.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key).id());
        }
    }

    /**
     * Test runnable.
     */
    private static class FailedCallable implements IgniteCallable<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final String ATTR_ATTEMPT = "Attempt";

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Key. */
        private final Object key;

        /** Call attempts. */
        private final Integer callAttempt;

        /**
         * @param key Key.
         * @param callAttempt Call attempts.
         */
        public FailedCallable(Object key, Integer callAttempt) {
            this.key = key;
            this.callAttempt = callAttempt;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws IgniteCheckedException {
            Integer attempt = jobCtx.getAttribute(ATTR_ATTEMPT);

            if (attempt == null)
                attempt = 1;

            assertEquals(ignite.affinity(NON_DFLT_CACHE_NAME).mapKeyToNode(key), ignite.cluster().localNode());

            jobCtx.setAttribute(ATTR_ATTEMPT, attempt + 1);

            if (attempt < callAttempt)
                throw new ComputeJobFailoverException("Failover exception.");
            else
                return attempt;
        }
    }

    /**
     * Test runnable.
     */
    private static class FailedRunnable implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final String ATTR_ATTEMPT = "Attempt";

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Key. */
        private final Object key;

        /** Call attempts. */
        private final Integer callAttempt;

        /**
         * @param key Key.
         * @param callAttempt Call attempts.
         */
        public FailedRunnable(Object key, Integer callAttempt) {
            this.key = key;
            this.callAttempt = callAttempt;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Integer attempt = jobCtx.getAttribute(ATTR_ATTEMPT);

            if (attempt == null)
                attempt = 1;

            assertEquals(ignite.affinity(NON_DFLT_CACHE_NAME).mapKeyToNode(key), ignite.cluster().localNode());

            jobCtx.setAttribute(ATTR_ATTEMPT, attempt + 1);

            if (attempt < callAttempt)
                throw new ComputeJobFailoverException("Failover exception.");
            else
                assertEquals(callAttempt, attempt);
        }
    }

    /**
     * Test callable.
     */
    private static class CheckCallable implements IgniteCallable<Object> {
        /** Affinity key. */
        private final Object affKey;

        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         * @param key Key.
         */
        private CheckCallable(Object affKey, Object key) {
            this.affKey = affKey;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(affKey).id());
            assert ignite.cluster().localNode().id().equals(ignite.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key).id());

            return null;
        }
    }
}
