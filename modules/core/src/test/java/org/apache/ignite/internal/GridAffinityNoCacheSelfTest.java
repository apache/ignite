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

package org.apache.ignite.internal;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.affinity.GridCacheAffinityImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests usage of affinity in case when cache doesn't exist.
 */
public class GridAffinityNoCacheSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String EXPECTED_MSG = "Failed to find cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityProxyNoCache() throws Exception {
        checkAffinityProxyNoCache(new Object());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityProxyNoCacheCacheObject() throws Exception {
        checkAffinityProxyNoCache(new TestCacheObject(new Object()));
    }

    /**
     * @param key Key.
     */
    private void checkAffinityProxyNoCache(Object key) {
        IgniteEx ignite = grid(0);

        final Affinity<Object> affinity = ignite.affinity("noCache");

        assertFalse("Affinity proxy instance expected", affinity instanceof GridCacheAffinityImpl);

        final ClusterNode n = ignite.cluster().localNode();

        assertAffinityMethodsException(affinity, key, n);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityImplCacheDeleted() throws Exception {
        checkAffinityImplCacheDeleted(new Object());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityImplCacheDeletedCacheObject() throws Exception {
        checkAffinityImplCacheDeleted(new TestCacheObject(new Object()));
    }

    /**
     * @param key Key.
     */
    private void checkAffinityImplCacheDeleted(Object key) throws InterruptedException {
        IgniteEx grid = grid(0);

        final String cacheName = "cacheToBeDeleted";

        grid(1).getOrCreateCache(cacheName);

        awaitPartitionMapExchange();

        Affinity<Object> affinity = grid.affinity(cacheName);

        assertTrue(affinity instanceof GridCacheAffinityImpl);

        final ClusterNode n = grid.cluster().localNode();

        grid.cache(cacheName).destroy();

        awaitPartitionMapExchange();

        assertAffinityMethodsException(grid.affinity(cacheName), key, n);
    }

    /**
     * @param affinity Affinity.
     * @param key Key.
     * @param n Node.
     */
    private void assertAffinityMethodsException(final Affinity<Object> affinity, final Object key,
        final ClusterNode n) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.affinityKey(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.allPartitions(n);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.backupPartitions(n);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.isBackup(n, key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.isPrimary(n, key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.isPrimaryOrBackup(n, key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapKeysToNodes(Collections.singleton(key));
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapKeyToPrimaryAndBackups(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapPartitionsToNodes(Collections.singleton(0));
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapPartitionToNode(0);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapPartitionToPrimaryAndBackups(0);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapKeyToNode(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.partition(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.partitions();
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.primaryPartitions(n);
            }
        }, IgniteException.class, EXPECTED_MSG);
    }

    /**
     */
    private static class TestCacheObject implements CacheObject {
        /** */
        private Object val;

        /**
         * @param val Value.
         */
        private TestCacheObject(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
            A.notNull(ctx, "ctx");

            return (T)val;
        }

        /** {@inheritDoc} */
        @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public byte cacheObjectType() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isPlatformType() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int putValue(long addr) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(final ByteBuffer buf, final int off, final int len)
            throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr)
            throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            throw new UnsupportedOperationException();
        }
    }
}
