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

import org.apache.ignite.internal.processors.query.index.IndexAbstractOperation;
import org.apache.ignite.internal.processors.query.index.IndexCreateOperation;
import org.apache.ignite.internal.processors.query.index.IndexDropOperation;
import org.apache.ignite.internal.processors.query.index.IndexAcceptDiscoveryMessage;
import org.apache.ignite.internal.processors.query.index.IndexFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.index.IndexProposeDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;
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
     * Default constructor.
     */
    public QueryIndexStates() {
        // No-op.
    }

    /**
     * Copy object.
     *
     * @return Copy.
     */
    public QueryIndexStates copy() {
        synchronized (mux) {
            QueryIndexStates res = new QueryIndexStates();

            for (Map.Entry<String, QueryIndexActiveOperation> activeOpEntry : activeOps.entrySet())
                res.activeOps.put(activeOpEntry.getKey(), activeOpEntry.getValue().copy());

            for (Map.Entry<String, QueryIndexState> readyOpEntry : readyOps.entrySet())
                res.readyOps.put(readyOpEntry.getKey(), readyOpEntry.getValue().copy());

            return res;
        }
    }

    /**
     * Try propose new index operation. Result is communicated through message error state.
     *
     * @param locNodeId Local node ID.
     * @param msg Propose message.
     */
    public void propose(UUID locNodeId, IndexProposeDiscoveryMessage msg) {
        synchronized (mux) {
            IndexAbstractOperation op = msg.operation();

            String idxName = op.indexName();

            if (activeOps.containsKey(idxName)) {
                msg.onError(locNodeId, "Failed to initiate index create/drop because another operation on the same " +
                    "index is in progress: " + idxName);
            }

            activeOps.put(idxName, new QueryIndexActiveOperation(op));
        }
    }

    /**
     * Process accept message propagating index from proposed to accepted state.
     *
     * @param msg Message.
     */
    public void accept(IndexAcceptDiscoveryMessage msg) {
        synchronized (mux) {
            IndexAbstractOperation op = msg.operation();

            String idxName = op.indexName();

            QueryIndexActiveOperation curOp = activeOps.get(idxName);

            if (curOp != null && F.eq(curOp.operation().operationId(), op.operationId())) {
                assert !curOp.accepted();

                curOp.accept();
            }
            else
                msg.onError("failed to apply dynamic index change operation because cache state changed " +
                    "concurrently.");
        }
    }

    /**
     * Process finish message.
     *
     * @param msg Message.
     */
    public void finish(IndexFinishDiscoveryMessage msg) {
        synchronized (mux) {
            IndexAbstractOperation op = msg.operation();

            String idxName = op.indexName();

            QueryIndexActiveOperation curOp = activeOps.remove(idxName);

            if (curOp != null) {
                if (F.eq(curOp.operation().operationId(), op.operationId())) {
                    if (!msg.hasError()) {
                        QueryIndexState state;

                        if (op instanceof IndexCreateOperation) {
                            IndexCreateOperation op0 = (IndexCreateOperation)op;

                            state = new QueryIndexState(op0.tableName(), idxName, ((IndexCreateOperation)op).index());
                        }
                        else {
                            assert op instanceof IndexDropOperation;

                            state = new QueryIndexState(null, idxName, null);
                        }

                        readyOps.put(idxName, state);
                    }
                }
            }
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
