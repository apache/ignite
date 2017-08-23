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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;

/** Context for DML operation on reducer node. */
class DistributedUpdateRun {
    /** Response counter. */
    private AtomicLong rspCntr;

    /** Update counter. */
    // TODO: Track concrete nodes instead of plain counter.
    private AtomicLong updCntr = new AtomicLong();

    /** Error keys. */
    private GridConcurrentHashSet<Object> errorKeys = new GridConcurrentHashSet<>();

    /** Query info. */
    private final GridRunningQueryInfo qry;

    /** Result future. */
    private GridFutureAdapter<UpdateResult> fut = new GridFutureAdapter<>();

    /**
     * Constructor.
     *
     * @param nodeCount Number of nodes to await results from.
     * @param qry Query info.
     */
    DistributedUpdateRun(int nodeCount, GridRunningQueryInfo qry) {
        rspCntr = new AtomicLong(nodeCount);

        this.qry = qry;
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
        // TODO: Snesible error message.
        fut.onDone(new IgniteCheckedException("Update failed.", e));
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
     * @return {@code true} if no more responses are expected.
     */
    boolean handleResponse(UUID id, GridH2DmlResponse msg) {
        boolean done = rspCntr.decrementAndGet() == 0;

        switch (msg.status()) {
            case GridH2DmlResponse.STATUS_ERR_KEYS:
                if (!F.isEmpty(msg.errorKeys()))
                    errorKeys.addAll(Arrays.asList(msg.errorKeys()));

            // Intentional fall-through.
            case GridH2DmlResponse.STATUS_OK:
                updCntr.addAndGet(msg.updateCounter());

                if (done)
                    fut.onDone(new UpdateResult(updCntr.get(), errorKeys.toArray()));

                return done;

            case GridH2DmlResponse.STATUS_ERROR:
                String err = msg.error();

                fut.onDone(new IgniteCheckedException("Update failed. " + (F.isEmpty(err)? "" : err) + "[reqId=" +
                    msg.requestId() + ", node=" + id + "]."));

                return false;

            default:
                fut.onDone(new IgniteCheckedException("Update failed. Invalid status in response message [reqId=" +
                    msg.requestId() + ", node=" + id + "]."));

                return false;
        }
    }
}
