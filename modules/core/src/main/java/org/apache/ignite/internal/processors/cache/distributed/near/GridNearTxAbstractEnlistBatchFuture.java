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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearTxAbstractEnlistBatchFuture<T> extends GridNearTxAbstractEnlistFuture<T> {
    /**
     * @param cctx    Cache context.
     * @param tx      Transaction.
     * @param timeout Timeout.
     * @param rdc     Compound future reducer.
     */
    protected GridNearTxAbstractEnlistBatchFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx, long timeout,
        @Nullable IgniteReducer<T, T> rdc) {
        super(cctx, tx, timeout, rdc);
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
