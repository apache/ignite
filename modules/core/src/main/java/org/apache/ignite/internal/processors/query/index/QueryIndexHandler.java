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

package org.apache.ignite.internal.processors.query.index;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Index state manager.
 */
public class QueryIndexHandler {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** All indexes. */
    private final Map<QueryIndexKey, Descriptor> idxs = new ConcurrentHashMap<>();

    /** Client futures. */
    private final Map<UUID, QueryIndexClientFuture> cliFuts = new ConcurrentHashMap<>();

    /** RW lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public QueryIndexHandler(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(QueryIndexHandler.class);
    }

    /**
     * Handle start.
     */
    public void onStart() {
        // TODO
    }

    /**
     * Handle kernal start callback.
     */
    public void onKernalStart() {
        // TODO
    }

    /**
     * Handle kernal stop callback.
     */
    public void onKernalStop() {
        // TODO
    }

    /**
     * Handle stop callback.
     */
    public void onStop() {
        // TODO
    }

    /**
     * Handle disconnect.
     */
    public void onDisconnected() {
        // TODO: Complete client futures, clear state.
    }

    /**
     * Handle cache creation.
     *
     * @param space Space.
     * @param typs Type descriptors.
     */
    public void onCacheCreated(String space, Collection<QueryTypeDescriptorImpl> typs) {
        lock.writeLock().lock();

        try {
            for (QueryTypeDescriptorImpl typ : typs) {
                for (QueryIndexDescriptorImpl idx : typ.indexes0()) {
                    QueryIndexKey idxKey = new QueryIndexKey(space, idx.name());

                    Descriptor desc = idxs.get(idxKey);

                    if (desc != null) {
                        throw new IgniteException("Duplicate index name [space=" + space + ", idxName=" + idx.name() +
                            ", existingTable=" + desc.type().tableName() + ", table=" + typ.tableName() + ']');
                    }

                    idxs.put(idxKey, new Descriptor(typ, idx));
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle cache stop.
     *
     * @param space Space.
     */
    public void onCacheStopped(String space) {
        lock.writeLock().lock();

        try {
            // Find matching indexes.
            Collection<QueryIndexKey> rmvIdxKeys = new HashSet<>();

            for (QueryIndexKey key : idxs.keySet()) {
                if (F.eq(space, key.space()))
                    rmvIdxKeys.add(key);
            }

            removeIndexes(rmvIdxKeys, false);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle type unregister.
     *
     * @param desc Descriptor.
     */
    public void onTypeUnregistered(QueryTypeDescriptorImpl desc) {
        lock.writeLock().lock();

        try {
            // Find matching indexes.
            Collection<QueryIndexKey> rmvKeys = new HashSet<>();

            for (Map.Entry<QueryIndexKey, Descriptor> idxEntry : idxs.entrySet()) {
                QueryIndexKey idxKey = idxEntry.getKey();
                Descriptor idxDesc = idxEntry.getValue();

                if (F.eq(desc, idxDesc.type()))
                    rmvKeys.add(idxKey);
            }

            removeIndexes(rmvKeys, true);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove indexes locally. Invoked when cache is either destroyed or type is unregistered.
     *
     * @param rmvIdxKeys Index keys to be removed.
     * @param typUnregister {@code True} if type was undeployed, {@code false} if cache was undeployed.
     */
    private void removeIndexes(Collection<QueryIndexKey> rmvIdxKeys, boolean typUnregister) {
        // Remove matched indexes
        for (QueryIndexKey rmvIdxKey : rmvIdxKeys) {
            // TODO: Callback to indexing SPI should be done from here
            idxs.remove(rmvIdxKey);
        }

        // Complete pending futures.
        Collection<UUID> rmvCliFutIds = new HashSet<>();

        for (Map.Entry<UUID, QueryIndexClientFuture> cliFutEntry : cliFuts.entrySet()) {
            UUID cliFutId = cliFutEntry.getKey();
            QueryIndexClientFuture cliFut = cliFutEntry.getValue();

            if (rmvIdxKeys.contains(cliFut.getKey()))
                rmvCliFutIds.add(cliFutId);
        }

        for (UUID rmvCliFutId : rmvCliFutIds) {
            QueryIndexClientFuture rmvCliFut = cliFuts.remove(rmvCliFutId);

            if (typUnregister)
                rmvCliFut.onTypeUnregistered();
            else
                rmvCliFut.onCacheStop();
        }
    }

    /**
     * Handle dynamic index creation.
     *
     * @param space Space.
     * @param tblName Table name.
     * @param idx Index.
     * @param ifNotExists IF-NOT-EXISTS flag.
     * @return Future completed when index is created.
     */
    public IgniteInternalFuture<?> onCreateIndex(String space, String tblName, QueryIndex idx,
        boolean ifNotExists) {
        String idxName = idx.getName() != null ? idx.getName() : QueryEntity.defaultIndexName(idx);

        QueryIndexKey idxKey = new QueryIndexKey(space, idxName);

        lock.readLock().lock();

        try {
            Descriptor oldIdxDesc = idxs.get(idxKey);

            if (oldIdxDesc != null) {
                // Make sure that index is bound to the same table.
                String oldTblName = oldIdxDesc.type().tableName();

                if (!F.eq(oldTblName, tblName)) {
                    return new GridFinishedFuture<>(new IgniteException("Index already exists and is bound to " +
                        "another table [space=" + space + ", idxName=" + idxName + ", expTblName=" + oldTblName +
                        ", actualTblName=" + tblName + ']'));
                }

                if (ifNotExists)
                    return new GridFinishedFuture<>();
                else
                    return new GridFinishedFuture<>(new IgniteException("Index already exists [space=" + space +
                        ", idxName=" + idxName + ']'));
            }

            UUID opId = UUID.randomUUID();
            QueryIndexClientFuture fut = new QueryIndexClientFuture(opId, idxKey);

            QueryIndexClientFuture oldFut = cliFuts.put(opId, fut);

            assert oldFut == null;

            // TODO: Start discovery.

            return fut;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Type and index descriptor.
     */
    private static class Descriptor {
        /** Type. */
        private final QueryTypeDescriptorImpl typ;

        /** Index. */
        private final QueryIndexDescriptorImpl idx;

        /**
         * Constructor.
         *
         * @param typ Type.
         * @param idx Index.
         */
        private Descriptor(QueryTypeDescriptorImpl typ, QueryIndexDescriptorImpl idx) {
            this.typ = typ;
            this.idx = idx;
        }

        /**
         * @return Type.
         */
        public QueryTypeDescriptorImpl type() {
            return typ;
        }

        /**
         * @return Index.
         */
        public QueryIndexDescriptorImpl index() {
            return idx;
        }
    }
}
