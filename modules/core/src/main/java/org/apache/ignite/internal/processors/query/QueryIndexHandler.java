package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;

import java.util.Collection;
import java.util.Iterator;
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
    private final Map<String, Descriptor> idxs = new ConcurrentHashMap<>();

    /** Client futures. */
    private final Map<UUID, GridFutureAdapter> cliFuts = new ConcurrentHashMap<>();

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
     * Handle cache creation.
     *
     * @param cacheName Cache name.
     * @param typs Type descriptors.
     */
    public void onCacheCreated(String cacheName, Collection<QueryTypeDescriptorImpl> typs) {
        lock.writeLock().lock();

        try {
            for (QueryTypeDescriptorImpl typ : typs) {
                for (QueryIndexDescriptorImpl idx : typ.indexes0()) {
                    Descriptor desc = idxs.get(idx.name());

                    if (desc != null) {
                        throw new IgniteException("Duplicate index name [idxName=" + idx.name() +
                            ", existingCache=" + desc.type().cacheName() + ", newCache=" + cacheName + ']');
                    }

                    idxs.put(idx.name(), new Descriptor(typ, idx));
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
     * @param cacheName Cache name.
     */
    public void onCacheStopped(String cacheName) {
        lock.writeLock().lock();

        try {
            Iterator<Map.Entry<String, Descriptor>> iter = idxs.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<String, Descriptor> entry = iter.next();

                if (F.eq(cacheName, entry.getValue().type().cacheName()))
                    iter.remove();
            }
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
            Iterator<Map.Entry<String, Descriptor>> iter = idxs.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<String, Descriptor> entry = iter.next();

                if (F.eq(desc, entry.getValue().type()))
                    iter.remove();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle disconnect.
     */
    public void onDisconnected() {
        // TODO
    }

    /**
     * Handle dynamic index creation.
     *
     * @param idx Index.
     * @param ifNotExists IF-NOT-EXISTS flag.
     * @return Future completed when index is created.
     */
    public IgniteInternalFuture<?> onCreateIndex(String cacheName, String tblName, QueryIndex idx,
        boolean ifNotExists) {
        String idxName = idx.getName() != null ? idx.getName() : QueryEntity.defaultIndexName(idx);

        lock.readLock().lock();

        try {
            Descriptor oldIdxDesc = idxs.get(idxName);

            if (oldIdxDesc != null) {
                // Make sure that index is bound to the same table.
                String oldTblName = oldIdxDesc.type().tableName();

                if (!F.eq(oldTblName, tblName)) {
                    return new GridFinishedFuture<>(new IgniteException("Index already exists and is bound to " +
                        "another table [idxName=" + idxName + ", expTblName=" + oldTblName +
                        ", actualTblName=" + tblName + ']'));
                }

                if (ifNotExists)
                    return new GridFinishedFuture<>();
                else
                    return new GridFinishedFuture<>(new IgniteException("Index already exists [idxName=" +
                        idxName + ']'));
            }

            UUID opId = UUID.randomUUID();
            GridFutureAdapter fut = new GridFutureAdapter();

            GridFutureAdapter oldFut = cliFuts.put(opId, fut);

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
    private static final class Descriptor {
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
