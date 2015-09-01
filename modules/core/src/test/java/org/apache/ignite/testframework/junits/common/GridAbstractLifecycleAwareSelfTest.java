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

package org.apache.ignite.testframework.junits.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.CacheNameResource;

/**
 * Base class for tests against {@link LifecycleAware} support.
 */
public abstract class GridAbstractLifecycleAwareSelfTest extends GridCommonAbstractTest {
    /** */
    protected Collection<TestLifecycleAware> lifecycleAwares = new ArrayList<>();

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestLifecycleAware implements LifecycleAware {
        /** */
        private AtomicInteger startCnt = new AtomicInteger();

        /** */
        private AtomicInteger stopCnt = new AtomicInteger();

        /** */
        @CacheNameResource
        private String cacheName;

        /** */
        private final String expCacheName;

        /**
         *
         */
        public TestLifecycleAware() {
            expCacheName = null;
        }

        /**
         * @param expCacheName Expected injected cache name.
         */
        public TestLifecycleAware(String expCacheName) {
            this.expCacheName = expCacheName;
        }

        /** {@inheritDoc} */
        @Override public void start() {
            startCnt.incrementAndGet();

            assertEquals("Unexpected cache name for " + this, expCacheName, cacheName);
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stopCnt.incrementAndGet();
        }

        /**
         * @return Number of times {@link LifecycleAware#start} was called.
         */
        public int startCount() {
            return startCnt.get();
        }

        /**
         * @return Number of times {@link LifecycleAware#stop} was called.
         */
        public int stopCount() {
            return stopCnt.get();
        }

        /**
         * @param cacheName Cache name.
         */
        public void cacheName(String cacheName) {
            this.cacheName = cacheName;
        }
    }

    /**
     * After grid start callback.
     * @param ignite Grid.
     */
    protected void afterGridStart(Ignite ignite) {
        // No-op.
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleAware() throws Exception {
        Ignite ignite = startGrid();

        afterGridStart(ignite);

        assertFalse(lifecycleAwares.isEmpty());

        for (TestLifecycleAware lifecycleAware : lifecycleAwares) {
            assertEquals("Unexpected start count for " + lifecycleAware, 1, lifecycleAware.startCount());
            assertEquals("Unexpected stop count for " + lifecycleAware, 0, lifecycleAware.stopCount());
        }

        try {
            stopGrid();

            for (TestLifecycleAware lifecycleAware : lifecycleAwares) {
                assertEquals("Unexpected start count for " + lifecycleAware, 1, lifecycleAware.startCount());
                assertEquals("Unexpected stop count for " + lifecycleAware, 1, lifecycleAware.stopCount());
            }
        }
        finally {
            lifecycleAwares.clear();
        }
    }
}