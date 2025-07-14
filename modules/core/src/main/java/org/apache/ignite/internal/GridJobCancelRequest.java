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

import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Job cancellation request.
 */
public class GridJobCancelRequest implements Message {
    /** */
    @Order(0)
    private IgniteUuid sessionId;

    /** */
    @Order(1)
    private IgniteUuid jobId;

    /** */
    @Order(2)
    private boolean system;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public GridJobCancelRequest() {
        // No-op.
    }

    /**
     * @param sessionId Task session ID.
     */
    public GridJobCancelRequest(IgniteUuid sessionId) {
        assert sessionId != null;

        this.sessionId = sessionId;
    }

    /**
     * @param sessionId Task session ID.
     * @param jobId Job ID.
     */
    public GridJobCancelRequest(@Nullable IgniteUuid sessionId, @Nullable IgniteUuid jobId) {
        assert sessionId != null || jobId != null;

        this.sessionId = sessionId;
        this.jobId = jobId;
    }

    /**
     * @param sessionId Session ID.
     * @param jobId Job ID.
     * @param system System flag.
     */
    public GridJobCancelRequest(@Nullable IgniteUuid sessionId, @Nullable IgniteUuid jobId, boolean system) {
        assert sessionId != null || jobId != null;

        this.sessionId = sessionId;
        this.jobId = jobId;
        this.system = system;
    }

    /**
     * Gets execution ID of task to be cancelled.
     *
     * @return Execution ID of task to be cancelled.
     */
    @Nullable public IgniteUuid sessionId() {
        return sessionId;
    }

    /**
     * Gets session ID of job to be cancelled. If {@code null}, then
     * all jobs for the specified task execution ID will be cancelled.
     *
     * @return Execution ID of job to be cancelled.
     */
    @Nullable public IgniteUuid jobId() {
        return jobId;
    }

    /**
     * @return {@code True} if request to cancel is sent out of system when task
     *       has already been reduced and further results are no longer interesting.
     */
    public boolean system() {
        return system;
    }

    /**
     * Gets execution ID of task to be cancelled.
     */
    public void sessionId(IgniteUuid sesId) {
        this.sessionId = sesId;
    }

    /**
     * Gets session ID of job to be cancelled. If {@code null}, then
     * all jobs for the specified task execution ID will be cancelled.
     */
    public void jobId(IgniteUuid jobId) {
        this.jobId = jobId;
    }

    /**
     */
    public void system(boolean system) {
        this.system = system;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobCancelRequest.class, this);
    }
}
