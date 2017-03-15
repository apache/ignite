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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.internal.processors.query.ddl.AbstractIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.CreateIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.DropIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.IndexAcceptDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.IndexFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.IndexProposeDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Dynamic index states.
 */
public class QueryIndexStates implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Currently running operations in either proposed or accepted states. */
    private final Map<String, QueryIndexActiveOperation> activeOps = new HashMap<>();

    /** Finished operations. */
    private final Map<String, QueryIndexState> readyOps = new HashMap<>();

    /** Mutext for state synchronization. */
    private final Object mux = new Object();

    /**
     * Try propose new index operation.
     *
     * @param locNodeId Local node ID.
     * @param msg Propose message.
     * @return {@code True} if propose succeeded.
     */
    public boolean propose(UUID locNodeId, IndexProposeDiscoveryMessage msg) {
        synchronized (mux) {
            AbstractIndexOperation op = msg.operation();

            String idxName = op.indexName();

            if (activeOps.containsKey(idxName)) {
                msg.onError(locNodeId, "Failed to initiate index create/drop because another operation on the same " +
                    "index is in progress: " + idxName);

                return false;
            }

            activeOps.put(idxName, new QueryIndexActiveOperation(op));

            return true;
        }
    }

    /**
     * Process accept message propagating index from proposed to accepted state.
     *
     * @param msg Message.
     * @return {@code True} if accept succeeded. It may fail in case of concurrent cache stop/start.
     */
    public boolean accept(IndexAcceptDiscoveryMessage msg) {
        synchronized (mux) {
            AbstractIndexOperation op = msg.operation();

            String idxName = op.indexName();

            QueryIndexActiveOperation curOp = activeOps.get(idxName);

            if (curOp != null) {
                if (F.eq(curOp.operation().operationId(), op.operationId())) {
                    assert !curOp.accepted();

                    curOp.accept();

                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Process finish message.
     *
     * @param msg Message.
     * @return {@code True} if accept succeeded. It may fail in case of concurrent cache stop/start.
     */
    public boolean finish(IndexFinishDiscoveryMessage msg) {
        synchronized (mux) {
            AbstractIndexOperation op = msg.operation();

            String tblName = op.tableName();
            String idxName = op.indexName();

            QueryIndexActiveOperation curOp = activeOps.remove(idxName);

            if (curOp != null) {
                if (F.eq(curOp.operation().operationId(), op.operationId())) {
                    if (!msg.hasError()) {
                        QueryIndexState state;

                        if (op instanceof CreateIndexOperation)
                            state = new QueryIndexState(tblName, idxName, ((CreateIndexOperation) op).index());
                        else {
                            assert op instanceof DropIndexOperation;

                            state = new QueryIndexState(tblName, idxName, null);
                        }

                        readyOps.put(idxName, state);
                    }

                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Get initial delta to be applied to the table.
     * @param tblName Table name.
     * @return Delta.
     */
    public Map<String, QueryIndex> initialDelta(String tblName) {
        synchronized (mux) {
            Map<String, QueryIndex> res = new HashMap<>();

            for (QueryIndexState idxState : readyOps.values()) {
                if (F.eq(tblName, idxState.tableName()))
                    res.put(idxState.indexName(), idxState.index());
            }

            for (Map.Entry<String, QueryIndexActiveOperation> op : activeOps.entrySet()) {
                if (op.getValue().accepted()) {
                    AbstractIndexOperation op0 = op.getValue().operation();

                    if (F.eq(tblName, op0.tableName())) {
                        QueryIndex idx;

                        if (op0 instanceof CreateIndexOperation)
                            idx = ((CreateIndexOperation)op0).index();
                        else {
                            assert op0 instanceof DropIndexOperation;

                            idx = null;
                        }

                        res.put(op0.indexName(), idx);
                    }
                }
            }

            return res;
        }
    }

    /**
     * @return Accepted active operations.
     */
    public Map<String, QueryIndexActiveOperation> acceptedActiveOperations() {
        synchronized (mux) {
            HashMap<String, QueryIndexActiveOperation> res = new HashMap<>();

            for (Map.Entry<String, QueryIndexActiveOperation> op : activeOps.entrySet()) {
                if (op.getValue().accepted())
                    res.put(op.getKey(), op.getValue());
            }

            return res;
        }
    }

    /**
     * @return Ready operations.
     */
    public Map<String, QueryIndexState> readyOperations() {
        synchronized (mux) {
            return new HashMap<>(readyOps);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexStates.class, this);
    }
}
