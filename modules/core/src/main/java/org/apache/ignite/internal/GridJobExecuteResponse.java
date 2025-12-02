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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution response.
 */
public class GridJobExecuteResponse implements Message {
    /** */
    @Order(0)
    private UUID nodeId;

    /** */
    @Order(value = 1, method = "sessionId")
    private IgniteUuid sesId;

    /** */
    @Order(2)
    private IgniteUuid jobId;

    /** Job result exception call holder. */
    @Order(value = 3, method = "gridExceptionHolder")
    private UserObject gridExHolder;

    /** Job result serialization call holder. */
    @Order(value = 4, method = "jobResultBytes")
    private @Nullable byte[] resBytes;

    /** */
    private @Nullable Object res;

    /** Job attributes serialization call holder. */
    @Order(value = 5, method = "jobAttrubutesBytes")
    private byte[] jobAttrsBytes;

    /** */
    private Map<Object, Object> jobAttrs;

    /** */
    @Order(value = 6, method = "cancelled")
    private boolean isCancelled;

    /** */
    @GridToStringExclude
    private IgniteException fakeEx;

    /** Retry topology version. */
    @Order(value = 7, method = "retryTopologyVersion")
    private AffinityTopologyVersion retry;

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
        this.gridExHolder = new UserObjectImpl(gridEx);
    }

    /**
     * @return Task session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /** */
    public void sessionId(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @return Job ID.
     */
    public IgniteUuid jobId() {
        return jobId;
    }

    /** */
    public void jobId(IgniteUuid jobId) {
        this.jobId = jobId;
    }

    /**
     * @return Serialized job result.
     */
    @Nullable public byte[] jobResultBytes() {
        return resBytes;
    }

    /** */
    public void jobResultBytes(@Nullable byte[] resBytes) {
        this.resBytes = resBytes;
    }

    /**
     * @return Job result.
     */
    @Nullable public Object getJobResult() {
        return res;
    }

    /**
     * @return Job exception.
     */
    @Nullable public IgniteException exception() {
        return (IgniteException)gridExHolder.get();
    }

    /**
     * @return Job exception holder.
     */
    @Nullable public UserObject gridExceptionHolder() {
        return gridExHolder;
    }

    /** */
    public void gridExceptionHolder(@Nullable UserObject gridExHolder) {
        this.gridExHolder = gridExHolder;
    }

    /**
     * @return Serialized job attributes.
     */
    @Nullable public byte[] jobAttrubutesBytes() {
        return jobAttrsBytes;
    }

    /** */
    public void jobAttrubutesBytes(@Nullable byte[] jobAttrsBytes) {
        this.jobAttrsBytes = jobAttrsBytes;
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

    /** */
    public void cancelled(boolean cancelled) {
        isCancelled = cancelled;
    }

    /**
     * @return Sender node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
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

    /** */
    public void retryTopologyVersion(@Nullable AffinityTopologyVersion retry) {
        this.retry = retry;
    }

    /**
     * Serializes user data to byte[] with provided marshaller.
     * Erases non-marshalled data like {@link #getJobAttributes()} or {@link #getJobResult()}.
     */
    public void marshallUserData(Marshaller marsh, @Nullable IgniteLogger log) {
        if (res != null) {
            try {
                resBytes = U.marshal(marsh, res);
            }
            catch (IgniteCheckedException e) {
                resBytes = null;

                String msg = "Failed to serialize job response [nodeId=" + nodeId +
                    ", ses=" + sesId + ", jobId=" + jobId +
                    ", resCls=" + (res == null ? null : res.getClass()) + ']';

                wrapSerializationError(e, msg, log);
            }

            res = null;
        }

        if (!F.isEmpty(jobAttrs)) {
            try {
                jobAttrsBytes = U.marshal(marsh, jobAttrs);
            }
            catch (IgniteCheckedException e) {
                jobAttrsBytes = null;

                String msg = "Failed to serialize job attributes [nodeId=" + nodeId +
                    ", ses=" + sesId + ", jobId=" + jobId +
                    ", attrs=" + jobAttrs + ']';

                wrapSerializationError(e, msg, log);
            }

            jobAttrs = null;
        }
    }

    /**
     * Deserializes user data from byte[] with provided marshaller and class loader.
     * Erases marshalled data like {@link #jobAttrubutesBytes()} or {@link #jobResultBytes()}.
     */
    public void unmarshallUserData(Marshaller marshaller, ClassLoader clsLdr) throws IgniteCheckedException {
        if (jobAttrsBytes != null) {
            jobAttrs = U.unmarshal(marshaller, jobAttrsBytes, clsLdr);

            jobAttrsBytes = null;
        }

        if (resBytes != null) {
            res = U.unmarshal(marshaller, resBytes, clsLdr);

            resBytes = null;
        }
    }

    /**
     * Wraps a user object serialization exception before sending a message over the network.
     *
     * <p>This method is used to handle exceptions that occur while serializing a user object
     * to be sent across the network. It ensures that the exception is properly recorded and
     * stored in {@code gridExHolder} as a {@link UserObjectImpl} instance.
     *
     * <p>Important notes:
     * <ul>
     *     <li>The holder {@code gridExHolder} is always set to a {@link UserObjectImpl} instance.
     *         It is <b>not</b> a proxy, because at this point we already know the object
     *         we are storing is an exception.</li>
     *     <li>If {@code gridExHolder} already contains a previous exception, it is added as
     *         a suppressed exception to the new {@link IgniteCheckedException}.</li>
     * </ul>
     *
     * @param e The serialization exception that occurred.
     * @param msg The log message to use if logging the error.
     * @param log Optional logger; may be null.
     */
    private void wrapSerializationError(IgniteCheckedException e, String msg, @Nullable IgniteLogger log) {
        if (gridExHolder != null)
            e.addSuppressed((IgniteException)gridExHolder.get());

        gridExHolder = new UserObjectImpl(U.convertException(e));

        if (log != null && (log.isDebugEnabled() || !X.hasCause(e, NodeStoppingException.class)))
            U.error(log, msg, e);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobExecuteResponse.class, this);
    }
}
