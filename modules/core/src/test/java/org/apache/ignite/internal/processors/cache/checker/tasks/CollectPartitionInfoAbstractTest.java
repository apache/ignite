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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base util methods for reconciliation partition testing.
 */
public class CollectPartitionInfoAbstractTest extends GridCommonAbstractTest {
    /**
     *
     */
    protected static final int EMPTY = 0;

    /** First partition. */
    protected static final int FIRST_PARTITION = 0;

    /**
     *
     */
    protected static final AtomicLong COUNTER = new AtomicLong();

    /**
     *
     */
    protected List<ComputeJobResult> valuesDiffVersion(int[][] keys,
        CacheObjectContext ctxo) throws IgniteCheckedException {
        return values(keys, ctxo, true);
    }

    /**
     *
     */
    protected List<ComputeJobResult> values(int[][] keys, CacheObjectContext ctxo) throws IgniteCheckedException {
        return values(keys, ctxo, false);
    }

    /**
     *
     */
    protected List<ComputeJobResult> values(int[][] keys, CacheObjectContext ctxo,
        boolean diffVer) throws IgniteCheckedException {
        List<ComputeJobResult> res = new ArrayList<>();

        GridCacheVersion ver = new GridCacheVersion();

        for (int[] partKeys : keys) {
            List<VersionedKey> partKeyVer = new ArrayList<>();
            UUID uuid = UUID.randomUUID();

            for (int key : partKeys) {
                if (diffVer)
                    ver = new GridCacheVersion(1, 1, COUNTER.incrementAndGet());

                if (key != EMPTY) {
                    partKeyVer.add(new VersionedKey(
                        uuid,
                        key(key, ctxo),
                        ver
                    ));
                }
            }

            res.add(new ComputeJobResultStub<>(partKeyVer));
        }

        return res;
    }

    /**
     *
     */
    protected KeyCacheObjectImpl key(Object key, CacheObjectContext ctxo) throws IgniteCheckedException {
        KeyCacheObjectImpl keyObj = new KeyCacheObjectImpl(key, null, 1);

        keyObj.prepareMarshal(ctxo);

        return keyObj;
    }

    /**
     *
     */
    protected static class ComputeJobResultStub<T> implements ComputeJobResult {
        /**
         *
         */
        private final List<T> data;

        /**
         *
         */
        private ComputeJobResultStub(List<T> data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobContext getJobContext() {
            throw new IllegalStateException("Unsupported!");
        }

        /** {@inheritDocl} */
        @Override public ExecutionResult<List<T>> getData() {
            return new ExecutionResult<>(data);
        }

        /** {@inheritDoc} */
        @Override public IgniteException getException() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T extends ComputeJob> T getJob() {
            throw new IllegalStateException("Unsupported!");
        }

        /** {@inheritDoc} */
        @Override public ClusterNode getNode() {
            throw new IllegalStateException("Unsupported!");
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            throw new IllegalStateException("Unsupported!");
        }
    }

    /**
     *
     */
    protected AffinityTopologyVersion lastTopologyVersion(IgniteEx node) throws IgniteCheckedException {
        return node.context().cache().context().exchange().lastTopologyFuture().get();
    }

    /**
     *
     */
    protected ClusterGroup group(IgniteEx node, Collection<ClusterNode> nodes) {
        return node.cluster().forNodeIds(nodes.stream().map(ClusterNode::id).collect(Collectors.toList()));
    }

    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     */
    protected void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        Object key
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                new GridCacheVersion(),
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }
}
