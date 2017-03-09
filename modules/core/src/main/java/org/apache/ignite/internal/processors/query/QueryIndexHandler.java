package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import java.util.Collection;
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

    /** Indexes. */
    private final Map<String, QueryIndexDescriptorImpl> idxs = new ConcurrentHashMap<>();

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
     * Handle cache creation.
     *
     * @param cacheName Cache name.
     * @param typDescs Type descriptors.
     */
    public void onCacheCreated(String cacheName, Collection<QueryTypeDescriptorImpl> typDescs) {
        // TODO: Make sure indexes are unique.
//        this.idxs.put(typ.indexes());
    }

    /**
     * Handle cache stop.
     *
     * @param cacheName Cache name.
     */
    public void onCacheStopped(String cacheName) {
        // TODO
    }

    /**
     * Handle type unregister.
     *
     * @param desc Descriptor.
     */
    public void onTypeUnregistered(QueryTypeDescriptorImpl desc) {
        // TODO
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
        // TODO: Integrated from previous impl:
//        for (QueryTypeDescriptorImpl desc : types.values()) {
//            if (desc.matchCacheAndTable(space, tblName))
//                return desc.dynamicIndexCreate(idx, ifNotExists);
//        }
//
//        return new GridFinishedFuture<>(new IgniteException("Failed to create index becase table is not found [" +
//            "space=" + space + ", table=" + tblName + ']'));


        lock.writeLock().lock();

        try {
            String idxName = idx.getName() != null ? idx.getName() : QueryEntity.defaultIndexName(idx);

            QueryIndexDescriptorImpl oldIdx = idxs.get(idxName);

            if (oldIdx != null) {
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
            lock.writeLock().unlock();
        }
    }
}
