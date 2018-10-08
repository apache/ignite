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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CacheMvccOperationChecksTest extends CacheMvccAbstractTest {
    /** Empty Class[]. */
    private final static Class[] E = new Class[]{};

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
    }

    /**
     * @throws Exception if failed.
     */
    public void testClearOperationsUnsupported() throws Exception {
        checkOperationUnsupported("clear", m("Clear"), E);

        checkOperationUnsupported("clearAsync", m("Clear"), E);

        checkOperationUnsupported("clear", m("Clear"), t(Object.class), 1);

        checkOperationUnsupported("clearAsync", m("Clear"), t(Object.class), 1);

        checkOperationUnsupported("clearAll", m("Clear"), t(Set.class), Collections.singleton(1));

        checkOperationUnsupported("clearAllAsync", m("Clear"), t(Set.class),
            Collections.singleton(1));
    }

    /**
     * @throws Exception if failed.
     */
    public void testLoadOperationsUnsupported() throws Exception {
        checkOperationUnsupported("loadCache", m("Load"), t(IgniteBiPredicate.class, Object[].class),
            P, new Object[]{ 1 });

        checkOperationUnsupported("loadCacheAsync", m("Load"), t(IgniteBiPredicate.class, Object[].class),
            P, new Object[]{ 1 });

        checkOperationUnsupported("localLoadCache", m("Load"), t(IgniteBiPredicate.class, Object[].class),
            P, new Object[]{ 1 });

        checkOperationUnsupported("localLoadCacheAsync", m("Load"), t(IgniteBiPredicate.class, Object[].class),
            P, new Object[]{ 1 });
    }

    /**
     * @throws Exception if failed.
     */
    public void testLockOperationsUnsupported() throws Exception {
        checkOperationUnsupported("lock", m("Lock"), t(Object.class), 1);

        checkOperationUnsupported("lockAll", m("Lock"), t(Collection.class), Collections.singleton(1));
    }

    /**
     * @throws Exception if failed.
     */
    public void testPeekOperationsUnsupported() throws Exception {
        checkOperationUnsupported("localPeek", m("Peek"), t(Object.class, CachePeekMode[].class), 1,
            new CachePeekMode[]{CachePeekMode.NEAR});
    }

    /**
     * @throws Exception if failed.
     */
    public void testEvictOperationsUnsupported() throws Exception {
        checkOperationUnsupported("localEvict", m("Evict"), t(Collection.class), Collections.singleton(1));
    }

    /**
     * @throws Exception if failed.
     */
    public void testWithExpiryPolicyUnsupported() throws Exception {
        checkOperationUnsupported("withExpiryPolicy", m("withExpiryPolicy"), t(ExpiryPolicy.class),
            EternalExpiryPolicy.factoryOf().create());
    }

    /**
     * @param opTypeName Operation type name.
     * @return Typical error message from {@link GridCacheAdapter}.
     */
    private static String m(String opTypeName) {
        return opTypeName + " operations are not supported on transactional caches when MVCC is enabled.";
    }

    /**
     * @param types Parameter types.
     * @return Types array.
     */
    private static Class[] t(Class... types) {
        return types;
    }

    /**
     * @param mtdName Method name.
     * @param errMsg Expected error message.
     * @param paramTypes Operation param types.
     * @param args Operation arguments.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkOperationUnsupported(String mtdName, String errMsg, Class[] paramTypes,
        Object... args) throws Exception {
        final boolean async = mtdName.endsWith("Async");

        try (final Ignite node = startGrid(0)) {
            final CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>("cache");

            cfg.setCacheMode(cacheMode());
            cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

            try (IgniteCache<Integer, String> cache = node.createCache(cfg)) {
                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @SuppressWarnings("unchecked")
                    @Override public Void call() throws Exception {
                        try {
                            Object o = U.invoke(null, cache, mtdName, paramTypes, args);

                            if (async) {
                                assertTrue(o instanceof IgniteFuture<?>);

                                ((IgniteFuture)o).get();
                            }
                        }
                        catch (Exception e) {
                            if (e.getCause() == null)
                                throw e;

                            if (e.getCause().getCause() == null)
                                throw e;

                            throw (Exception)e.getCause().getCause();
                        }

                        return null;
                    }
                }, UnsupportedOperationException.class, errMsg);
            }
        }
    }

    /**
     *
     */
    private final static IgniteBiPredicate<Object, Object> P = new IgniteBiPredicate<Object, Object>() {
        @Override public boolean apply(Object o, Object o2) {
            return false;
        }
    };
}
