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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class AbstractDeadlockDetectionTest extends GridCommonAbstractTest {
    /**
     * Checks that transactions and futures are completed and entries are not locked.
     * @param involvedKeys Involved keys.
     */
    protected void checkAllTransactionsCompleted(Set<Object> involvedKeys, int nodesCnt, String cacheName) {
        boolean fail = false;

        for (int i = 0; i < nodesCnt; i++) {
            Ignite ignite = ignite(i);

            int cacheId = ((IgniteCacheProxy)ignite.cache(cacheName)).context().cacheId();

            GridCacheSharedContext<Object, Object> cctx = ((IgniteKernal)ignite).context().cache().context();

            IgniteTxManager txMgr = cctx.tm();

            Collection<IgniteInternalTx> activeTxs = txMgr.activeTransactions();

            for (IgniteInternalTx tx : activeTxs) {
                Collection<IgniteTxEntry> entries = tx.allEntries();

                for (IgniteTxEntry entry : entries) {
                    if (entry.cacheId() == cacheId) {
                        fail = true;

                        U.error(log, "Transaction still exists: " + "\n" + tx.xidVersion() +
                            "\n" + tx.nearXidVersion() + "\n nodeId=" + cctx.localNodeId() + "\n tx=" + tx);
                    }
                }
            }

            Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

            assertTrue(futs.isEmpty());

            GridCacheAdapter<Object, Integer> intCache = internalCache(i, cacheName);

            GridCacheConcurrentMap map = intCache.map();

            for (Object key : involvedKeys) {
                KeyCacheObject keyCacheObj = intCache.context().toCacheKeyObject(key);

                GridCacheMapEntry entry = map.getEntry(intCache.context(), keyCacheObj);

                if (entry != null)
                    assertNull("Entry still has locks " + entry, entry.mvccAllLocal());
            }
        }

        if (fail)
            fail("Some transactions still exist");
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     */
    protected <T> List<T> primaryKeys(IgniteCache<?, ?> cache, final int cnt, final T startFrom) {
        return findPrimaryKeys(cache, cnt, startFrom);
    }

    /**
     * @param cache Cache.
     * @return Key for which given cache is primary.
     * @throws IgniteCheckedException If failed.
     */
    protected <T> T primaryKey(IgniteCache<?, ?> cache, T startFrom) throws IgniteCheckedException {
        return primaryKeys(cache, 1, startFrom).get(0);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given node is primary.
     */
    private <T> List<T> findPrimaryKeys(IgniteCache<?, ?> cache, final int cnt, final T startFrom) {
        A.ensure(cnt > 0, "cnt");

        final List<T> found = new ArrayList<>(cnt);

        final ClusterNode locNode = localNode(cache);

        final Affinity<T> aff = (Affinity<T>)affinity(cache);

        try {
            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    T key = startFrom;

                    for (int i = 0; i < 100_000; i++) {
                        if (aff.isPrimary(locNode, key)) {
                            if (!found.contains(key))
                                found.add(key);

                            if (found.size() == cnt)
                                return true;
                        }

                        key = (T)incrementKey(key, 1);
                    }

                    return false;
                }
            }, 5000);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        if (found.size() != cnt)
            throw new IgniteException("Unable to find " + cnt + " required keys.");

        return found;
    }

    /**
     * This method added in order to not change the behavior of this test after refactoring.
     *
     * @param key Key.
     * @param i Delta.
     * @return Incremented key.
     */
    protected Object incrementKey(Object key, int i) {
        if (key instanceof Integer) {
            Integer v = (Integer)key;

            return v + i;
        }
        else if (key instanceof IncrementalTestObject)
            return ((IncrementalTestObject)key).increment(i);
        else
            throw new IgniteException("Unable to increment objects of class " + key.getClass().getName() + ".");
    }

    /**
     * Wait for late affinity assignment after cache start.
     * So we can be sure that there will not happen unpredictable PME.
     *
     * @param minorTopVer Minor topology version before cache start.
     */
    void waitForLateAffinityAssignment(int minorTopVer) throws IgniteInterruptedCheckedException {
        assertTrue("Failed to wait for late affinity assignment",
            GridTestUtils.waitForCondition(() ->
                    grid(0).context().discovery().topologyVersionEx().minorTopologyVersion() == minorTopVer + 1,
                10_000));
    }

    /**
     *
     */
    protected static class KeyObject implements IncrementalTestObject {
        /** Id. */
        private int id;

        /** Name. */
        private String name;

        /**
         * @param id Id.
         */
        public KeyObject(int id) {
            this.id = id;
            this.name = "KeyObject" + id;
        }

        /** {@inheritDoc} */
        @Override public IncrementalTestObject increment(int times) {
            return new KeyObject(id + times);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "KeyObject{" + "id=" + id + ", name='" + name + "\'}";
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyObject obj = (KeyObject)o;

            return id == obj.id && name.equals(obj.name);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     * Interface for classes which can be incremented.
     */
    protected interface IncrementalTestObject {
        /**
         * @param times Number of increments.
         * @return {@code this}.
         */
        IncrementalTestObject increment(int times);
    }
}
