package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Index state manager.
 */
public class QueryIndexHandler {
    /** Indexes. */
    private final Map<String, GridQueryProcessor.IndexDescriptor> idxs = new ConcurrentHashMap<>();

    /** Client futures. */
    private final Map<UUID, GridFutureAdapter> cliFuts = new ConcurrentHashMap<>();

    /** RW lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Handle initial index state.
     *
     * @param idxs Indexes.
     */
    public void onInitialStateReady(Map<String, GridQueryProcessor.IndexDescriptor> idxs) {
        this.idxs.putAll(idxs);
    }

    /**
     * Handle dynamic index creation.
     *
     * @param idx Index.
     * @param ifNotExists IF-NOT-EXISTS flag.
     * @return Future completed when index is created.
     */
    public IgniteInternalFuture<?> onCreateIndex(QueryIndex idx, boolean ifNotExists) {
        lock.writeLock().lock();

        try {
            String idxName = idx.getName() != null ? idx.getName() : QueryEntity.defaultIndexName(idx);

            GridQueryProcessor.IndexDescriptor oldIdx = idxs.get(idxName);

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
