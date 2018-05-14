package org.apache.ignite.internal.processors.query.h2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistAbstractFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Future to process whole local result set of SELECT FOR UPDATE query.
 */
public class ResultSetEnlistFuture extends GridDhtTxQueryEnlistAbstractFuture<ResultSetEnlistFuture.ResultSetEnlistFutureResult>
    implements UpdateSourceIterator<Object> {
    /** */
    private static final long serialVersionUID = -8995895504338661551L;
    /** */
    private final ResultSet rs;

    /** */
    private final int keyColIdx;

    /** */
    private Boolean hasNext;

    /**
     * @param nearNodeId   Near node ID.
     * @param nearLockVer  Near lock version.
     * @param topVer       Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId     Thread ID.
     * @param nearFutId    Near future id.
     * @param nearMiniId   Near mini future id.
     * @param parts        Partitions.
     * @param tx           Transaction.
     * @param timeout      Lock acquisition timeout.
     * @param cctx         Cache context.
     * @param rs           Result set to process.
     */
    public ResultSetEnlistFuture(UUID nearNodeId, GridCacheVersion nearLockVer, AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot, long threadId, IgniteUuid nearFutId, int nearMiniId, @Nullable int[] parts,
        GridDhtTxLocalAdapter tx, long timeout, GridCacheContext<?, ?> cctx, ResultSet rs) {
        super(nearNodeId, nearLockVer, topVer, mvccSnapshot, threadId, nearFutId, nearMiniId, parts, tx, timeout, cctx);

        this.rs = rs;

        try {
            keyColIdx = rs.getMetaData().getColumnCount();
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
    }

    /**
     * @return Transaction adapter.
     */
    public GridDhtTxLocalAdapter tx() {
        return tx;
    }

    /** {@inheritDoc} */
    @Override protected UpdateSourceIterator<?> createIterator() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public ResultSetEnlistFutureResult createResponse(@NotNull Throwable err) {
        return new ResultSetEnlistFutureResult(0, err);
    }

    /** {@inheritDoc} */
    @Override public ResultSetEnlistFutureResult createResponse() {
        return new ResultSetEnlistFutureResult(cnt, null);
    }

    /** {@inheritDoc} */
    @Override protected void processEntry(GridDhtCacheEntry entry, GridCacheOperation op, CacheObject val) {
        synchronized (tx) {
            super.processEntry(entry, op, val);
        }
    }

    @Override public GridCacheOperation operation() {
        return GridCacheOperation.READ;
    }

    @Override public boolean hasNextX() {
        try {
            if (hasNext == null)
                hasNext = rs.next();

            return hasNext;
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
    }

    @Override public Object nextX() {
        if (!hasNextX())
            throw new NoSuchElementException();

        try {
            return rs.getObject(keyColIdx);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
        finally {
            hasNext = null;
        }
    }

    /**
     *
     */
    public static class ResultSetEnlistFutureResult {
        /** */
        private long cnt;

        /** */
        private Throwable err;


        /**
         * @param cnt Total rows counter on given node.
         * @param err Exception.
         */
        public ResultSetEnlistFutureResult(long cnt, Throwable err) {
            this.err = err;
            this.cnt = cnt;
        }

        /**
         * @return Total rows counter on given node.
         */
        public long enlistedRows() {
            return cnt;
        }

        /**
         * @return Exception.
         */
        public Throwable error() {
            return err;
        }
    }
}
