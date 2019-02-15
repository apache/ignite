/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Context for DML operation on reducer node.
 */
class DistributedUpdateRun {
    /** Expected number of responses. */
    private final int nodeCount;

    /** Registers nodes that have responded. */
    private final HashSet<UUID> rspNodes;

    /** Accumulates total number of updated rows. */
    private long updCntr = 0L;

    /** Accumulates error keys. */
    private HashSet<Object> errorKeys;

    /** Query info. */
    private final GridRunningQueryInfo qry;

    /** Result future. */
    private final GridFutureAdapter<UpdateResult> fut = new GridFutureAdapter<>();

    /**
     * Constructor.
     *
     * @param nodeCount Number of nodes to await results from.
     * @param qry Query info.
     */
    DistributedUpdateRun(int nodeCount, GridRunningQueryInfo qry) {
        this.nodeCount = nodeCount;
        this.qry = qry;

        rspNodes = new HashSet<>(nodeCount);
    }

    /**
     * @return Query info.
     */
    GridRunningQueryInfo queryInfo() {
        return qry;
    }

    /**
     * @return Result future.
     */
    GridFutureAdapter<UpdateResult> future() {
        return fut;
    }

    /**
     * Handle disconnection.
     * @param e Pre-formatted error.
     */
    void handleDisconnect(CacheException e) {
        fut.onDone(new IgniteCheckedException("Update failed because client node have disconnected.", e));
    }

    /**
     * Handle leave of a node.
     *
     * @param nodeId Node id.
     */
    void handleNodeLeft(UUID nodeId) {
        fut.onDone(new IgniteCheckedException("Update failed because map node left topology [nodeId=" + nodeId + "]"));
    }

    /**
     * Handle response from remote node.
     *
     * @param id Node id.
     * @param msg Response message.
     */
    void handleResponse(UUID id, GridH2DmlResponse msg) {
        synchronized (this) {
            if (!rspNodes.add(id))
                return; // ignore duplicated messages

            String err = msg.error();

            if (err != null) {
                fut.onDone(new IgniteCheckedException("Update failed. " + (F.isEmpty(err) ? "" : err) + "[reqId=" +
                    msg.requestId() + ", node=" + id + "]."));

                return;
            }

            if (!F.isEmpty(msg.errorKeys())) {
                List<Object> errList = Arrays.asList(msg.errorKeys());

                if (errorKeys == null)
                    errorKeys = new HashSet<>(errList);
                else
                    errorKeys.addAll(errList);
            }

            updCntr += msg.updateCounter();

            if (rspNodes.size() == nodeCount)
                fut.onDone(new UpdateResult(updCntr, errorKeys == null ? null : errorKeys.toArray()));
        }
    }
}
