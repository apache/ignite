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

import org.apache.ignite.internal.processors.query.ddl.AbstractIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.CreateIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.DropIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.IndexAcceptDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.IndexFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.IndexProposeDiscoveryMessage;
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

    /**
     * Try propose new index operation.
     *
     * @param locNodeId Local node ID.
     * @param msg Propose message.
     * @return {@code True} if propose succeeded.
     */
    public boolean propose(UUID locNodeId, IndexProposeDiscoveryMessage msg) {
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

    /**
     * Process accept message propagating index from proposed to accepted state.
     *
     * @param msg Message.
     */
    public void accept(IndexAcceptDiscoveryMessage msg) {
        AbstractIndexOperation op = msg.operation();

        String idxName = op.indexName();

        QueryIndexActiveOperation curOp = activeOps.get(idxName);

        assert curOp != null && !curOp.accepted(); // Operation is found and is in proposed ("false") state.
        assert F.eq(curOp.operation().operationId(), op.operationId()); // Operation ID matches.

        curOp.accept();
    }

    /**
     * Process finish message.
     *
     * @param msg Message.
     */
    @SuppressWarnings("ConstantConditions")
    public void finish(IndexFinishDiscoveryMessage msg) {
        AbstractIndexOperation op = msg.operation();

        String idxName = op.indexName();

        QueryIndexActiveOperation curOp = activeOps.remove(idxName);

        assert curOp != null; // Operation is found.
        assert F.eq(curOp.operation().operationId(), op.operationId()); // Operation ID matches.

        if (!msg.hasError()) {
            QueryIndexState state;

            if (op instanceof CreateIndexOperation)
                state = new QueryIndexState(idxName, ((CreateIndexOperation)op).index());
            else {
                assert op instanceof DropIndexOperation;

                state = new QueryIndexState(idxName, null);
            }

            readyOps.put(idxName, state);
        }
    }

    /**
     * @return Ready operations.
     */
    public Map<String, QueryIndexState> readyOperation() {
        return new HashMap<>(readyOps);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexStates.class, this);
    }
}
