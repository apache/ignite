/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.common;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Base class for tests against {@link org.gridgain.grid.GridLifecycleAware} support.
 */
public abstract class GridAbstractLifecycleAwareSelfTest extends GridCommonAbstractTest {
    /** */
    protected Collection<TestLifecycleAware> lifecycleAwares = new ArrayList<>();

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestLifecycleAware implements GridLifecycleAware {
        /** */
        private AtomicInteger startCnt = new AtomicInteger();

        /** */
        private AtomicInteger stopCnt = new AtomicInteger();

        /** */
        @GridCacheNameResource
        private String cacheName;

        /** */
        private final String expCacheName;

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
         * @return Number of times {@link GridLifecycleAware#start} was called.
         */
        public int startCount() {
            return startCnt.get();
        }

        /**
         * @return Number of times {@link GridLifecycleAware#stop} was called.
         */
        public int stopCount() {
            return stopCnt.get();
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
