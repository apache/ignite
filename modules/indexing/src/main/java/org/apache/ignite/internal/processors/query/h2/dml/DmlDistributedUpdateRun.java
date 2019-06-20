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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Context for DML operation on reducer node.
 */
public class DmlDistributedUpdateRun {
    /** Expected number of responses. */
    private final int nodeCount;

    /** Registers nodes that have responded. */
    private final HashSet<UUID> rspNodes;

    /** Accumulates total number of updated rows. */
    private long updCntr = 0L;

    /** Accumulates error keys. */
    private HashSet<Object> errorKeys;

    /** Result future. */
    private final GridFutureAdapter<UpdateResult> fut = new GridFutureAdapter<>();

    /**
     * Constructor.
     *
     * @param nodeCount Number of nodes to await results from.
     */
    public DmlDistributedUpdateRun(int nodeCount) {
        this.nodeCount = nodeCount;

        rspNodes = new HashSet<>(nodeCount);
    }

    /**
     * @return Result future.
     */
    public GridFutureAdapter<UpdateResult> future() {
        return fut;
    }

    /**
     * Handle disconnection.
     * @param e Pre-formatted error.
     */
    public void handleDisconnect(CacheException e) {
        fut.onDone(new IgniteCheckedException("Update failed because client node have disconnected.", e));
    }

    /**
     * Handle leave of a node.
     *
     * @param nodeId Node id.
     */
    public void handleNodeLeft(UUID nodeId) {
        fut.onDone(new IgniteCheckedException("Update failed because map node left topology [nodeId=" + nodeId + "]"));
    }

    /**
     * Handle response from remote node.
     *
     * @param id Node id.
     * @param msg Response message.
     */
    public void handleResponse(UUID id, GridH2DmlResponse msg) {
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
