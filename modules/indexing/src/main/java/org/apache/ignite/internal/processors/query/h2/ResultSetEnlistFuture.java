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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.DhtLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Future to process whole local result set of SELECT FOR UPDATE query.
 */
public interface ResultSetEnlistFuture extends DhtLockFuture<Long> {
    /**
     * @param rs Result set.
     * @return Update source.
     */
    static UpdateSourceIterator<?> createIterator(ResultSet rs) {
        return new ResultSetUpdateSourceIteratorWrapper(rs);
    }

    /** */
    void init();

    /**
     *
     * @param nearNodeId   Near node ID.
     * @param nearLockVer  Near lock version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId     Thread ID.
     * @param nearFutId    Near future id.
     * @param nearMiniId   Near mini future id.
     * @param parts        Partitions.
     * @param tx           Transaction.
     * @param timeout      Lock acquisition timeout.
     * @param cctx         Cache context.
     * @param rs           Result set to process.
     * @return Result set enlist future.
     */
    static ResultSetEnlistFuture future(UUID nearNodeId, GridCacheVersion nearLockVer,
        MvccSnapshot mvccSnapshot, long threadId, IgniteUuid nearFutId, int nearMiniId, @Nullable int[] parts,
        GridDhtTxLocalAdapter tx, long timeout, GridCacheContext<?, ?> cctx, ResultSet rs) {

        if (tx.near())
            return new NearResultSetEnlistFuture(nearNodeId, nearLockVer, mvccSnapshot, threadId, nearFutId, nearMiniId, parts, tx, timeout, cctx, rs);
        else
            return new DhtResultSetEnlistFuture(nearNodeId, nearLockVer, mvccSnapshot, threadId, nearFutId, nearMiniId, parts, tx, timeout, cctx, rs);
    }

    /**
     *
     */
    public static class ResultSetUpdateSourceIteratorWrapper implements UpdateSourceIterator<Object> {
        /** */
        private static final long serialVersionUID = -8745196216234843471L;

        /** */
        private final ResultSet rs;

        /** */
        private Boolean hasNext;

        /** */
        private int keyColIdx;

        /**
         * @param rs Result set.
         */
        public ResultSetUpdateSourceIteratorWrapper(ResultSet rs) {
            this.rs = rs;
            keyColIdx = -1;
        }

        /** {@inheritDoc} */
        @Override public EnlistOperation operation() {
            return EnlistOperation.LOCK;
        }

        /** {@inheritDoc} */
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

        /** {@inheritDoc} */
        @Override public Object nextX() {
            if (!hasNextX())
                throw new NoSuchElementException();

            try {
                if (keyColIdx == -1)
                    keyColIdx = rs.getMetaData().getColumnCount();

                return rs.getObject(keyColIdx);
            }
            catch (SQLException e) {
                throw new IgniteSQLException(e);
            }
            finally {
                hasNext = null;
            }
        }
    }
}
