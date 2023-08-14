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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearTxAbstractEnlistBatchFuture<T> extends GridNearTxAbstractEnlistFuture<T> {
    /** Default batch size. */
    protected static final int DFLT_BATCH_SIZE = 1024;

    /** SkipCntr field updater. */
    protected static final AtomicIntegerFieldUpdater<GridNearTxAbstractEnlistBatchFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxAbstractEnlistBatchFuture.class, "skipCntr");

    /** Marker object. */
    protected static final Object FINISHED = new Object();

    /** */
    protected final UpdateSourceIterator<?> it;

    /** */
    protected final Map<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** */
    protected final boolean sequential;

    /** */
    protected final AtomicInteger batchCntr = new AtomicInteger();

    /** Batch size. */
    protected final int batchSize;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    protected volatile int skipCntr;

    /** Topology locked flag. */
    protected boolean topLocked;

    /** Row extracted from iterator but not yet used. */
    protected Object peek;

    /**
     * @param cctx    Cache context.
     * @param tx      Transaction.
     * @param timeout Timeout.
     * @param rdc     Compound future reducer.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     * @param sequential Sequential locking flag.
     */
    protected GridNearTxAbstractEnlistBatchFuture(
        GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout,
        @Nullable IgniteReducer<T, T> rdc,
        UpdateSourceIterator<?> it,
        int batchSize,
        boolean sequential) {
        super(cctx, tx, timeout, rdc);

        this.it = it;
        this.batchSize = batchSize > 0 ? batchSize : DFLT_BATCH_SIZE;
        this.sequential = sequential;
    }

    /**
     * A batch of rows
     */
    protected static class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** Rows. */
        private final List<Object> rows = new ArrayList<>();

        /** Local backup rows. */
        private List<Object> locBkpRows;

        /** Readiness flag. Set when batch is full or no new rows are expected. */
        private boolean ready;

        /**
         * @param node Cluster node.
         */
        protected Batch(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * Adds a row.
         *
         * @param row Row.
         * @param locBackup {@code true}, when the row key has local backup.
         */
        public void add(Object row, boolean locBackup) {
            rows.add(row);

            if (locBackup) {
                if (locBkpRows == null)
                    locBkpRows = new ArrayList<>();

                locBkpRows.add(row);
            }
        }

        /**
         * @return number of rows.
         */
        public int size() {
            return rows.size();
        }

        /**
         * @return Collection of rows.
         */
        public Collection<Object> rows() {
            return rows;
        }

        /**
         * @return Collection of local backup rows.
         */
        public List<Object> localBackupRows() {
            return locBkpRows;
        }

        /**
         * @return Readiness flag.
         */
        public boolean ready() {
            return ready;
        }

        /**
         * Sets readiness flag.
         *
         * @param ready Flag value.
         */
        public void ready(boolean ready) {
            this.ready = ready;
        }
    }
}
