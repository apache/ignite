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

package org.apache.ignite.internal;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution response.
 */
public class GridJobExecuteResponse implements DeferredUnmarshalMessage {
    /** */
    @Order(0)
    UUID nodeId;

    /** */
    @Order(1)
    IgniteUuid sesId;

    /** */
    @Order(2)
    IgniteUuid jobId;

    /** */
    @GridToStringExclude
    @Marshalled("gridExBytes")
    @Nullable IgniteException gridEx;

    /** */
    @Order(3)
    @Nullable byte[] gridExBytes;

    /** */
    @GridToStringExclude
    @Marshalled("resBytes")
    @Nullable Object res;

    /** */
    @Order(4)
    @Nullable byte[] resBytes;

    /** */
    @GridToStringExclude
    @Marshalled("jobAttrsBytes")
    Map<Object, Object> jobAttrs;

    /** */
    @Order(5)
    byte[] jobAttrsBytes;

    /** */
    @Order(6)
    boolean isCancelled;

    /** */
    @GridToStringExclude
    private IgniteException fakeEx;

    /** Retry topology version. */
    @Order(7)
    AffinityTopologyVersion retry;

    /**
     * Default constructor.
     */
    public GridJobExecuteResponse() {
        // No-op.
    }

    /**
     * @param nodeId Sender node ID.
     * @param sesId Task session ID
     * @param jobId Job ID.
     * @param gridEx Grid exception.
     * @param res Result.
     * @param jobAttrs Job attributes.
     * @param isCancelled Whether job was cancelled or not.
     * @param retry Topology version for that partitions haven't been reserved on the affinity node.
     */
    public GridJobExecuteResponse(UUID nodeId,
        IgniteUuid sesId,
        IgniteUuid jobId,
        @Nullable IgniteException gridEx,
        @Nullable Object res,
        Map<Object, Object> jobAttrs,
        boolean isCancelled,
        AffinityTopologyVersion retry
    ) {
        assert nodeId != null;
        assert sesId != null;
        assert jobId != null;

        this.nodeId = nodeId;
        this.sesId = sesId;
        this.jobId = jobId;
        this.res = res;
        this.jobAttrs = jobAttrs;
        this.isCancelled = isCancelled;
        this.retry = retry;
        this.gridEx = gridEx;
    }

    /**
     * @return Task session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @return Job ID.
     */
    public IgniteUuid jobId() {
        return jobId;
    }

    /**
     * @return Job result.
     */
    @Nullable public Object getJobResult() {
        return res;
    }

    /** @return Job exception. */
    @Nullable public IgniteException exception() {
        return gridEx;
    }

    /**
     * @return Job attributes.
     */
    @Nullable public Map<Object, Object> getJobAttributes() {
        return jobAttrs;
    }

    /**
     * @return Job cancellation status.
     */
    public boolean cancelled() {
        return isCancelled;
    }

    /**
     * @return Sender node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Fake exception.
     */
    public IgniteException getFakeException() {
        return fakeEx;
    }

    /**
     * @param fakeEx Fake exception.
     */
    public void setFakeException(IgniteException fakeEx) {
        this.fakeEx = fakeEx;
    }

    /**
     * @return {@code True} if need retry job.
     */
    public boolean retry() {
        return retry != null;
    }

    /**
     * @return Topology version for that specified partitions haven't been reserved
     *          on the affinity node.
     */
    public @Nullable AffinityTopologyVersion retryTopologyVersion() {
        return retry;
    }

    /** @return A copy carrying {@code err} and no payload. */
    public GridJobExecuteResponse withError(IgniteException err) {
        return new GridJobExecuteResponse(nodeId, sesId, jobId, err, null, null, isCancelled, retry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobExecuteResponse.class, this);
    }
}
