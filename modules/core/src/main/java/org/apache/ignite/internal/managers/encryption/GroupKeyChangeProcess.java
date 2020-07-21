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

package org.apache.ignite.internal.managers.encryption;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager.EmptyResult;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager.KeyChangeFuture;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CACHE_GROUP_KEY_CHANGE_FINISH;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CACHE_GROUP_KEY_CHANGE_PREPARE;

/**
 * Two phase distributed process, that performs cache group encryption key rotation.
 */
class GroupKeyChangeProcess {
    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Cache group encyption key change prepare phase. */
    private final DistributedProcess<ChangeCacheEncryptionRequest, EmptyResult> prepareGKChangeProc;

    /** Cache group encyption key change perform phase. */
    private final DistributedProcess<ChangeCacheEncryptionRequest, EmptyResult> performGKChangeProc;

    /** Cache group key change future. */
    private volatile GroupKeyChangeFuture fut;

    /** Cache group key change request. */
    private volatile ChangeCacheEncryptionRequest req;

    /**
     * @param ctx Grid kernal context.
     */
    GroupKeyChangeProcess(GridKernalContext ctx, Object opsMux) {
        this.ctx = ctx;

        prepareGKChangeProc =
            new DistributedProcess<>(ctx, CACHE_GROUP_KEY_CHANGE_PREPARE, this::prepare, this::finishPrepare);
        performGKChangeProc =
            new DistributedProcess<>(ctx, CACHE_GROUP_KEY_CHANGE_FINISH, this::perform, this::finishPerform);
    }

    /**
     * @return {@code True} if operation is still in progress.
     */
    public boolean started() {
        return req != null;
    }

    /**
     * @return {@code True} if operation is not finished.
     */
    public boolean finished() {
        IgniteInternalFuture<Void> fut0 = fut;

        return fut0 == null || fut0.isDone();
    }

    /**
     * @param msg Error message.
     */
    public void cancel(String msg) {
        GridFutureAdapter<Void> keyChangeFut = fut;

        if (keyChangeFut != null && !keyChangeFut.isDone())
            keyChangeFut.onDone(new IgniteFutureCancelledException(msg));
    }

    /**
     * @param req Request.
     */
    public IgniteFuture<Void> start(ChangeCacheEncryptionRequest req) {
        if (!finished()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Cache group key change was rejected. " +
                "The previous change was not completed."));
        }

        fut = new GroupKeyChangeFuture(req);

        prepareGKChangeProc.start(req.requestId(), req);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Validates existing keys.
     *
     * @param req Request.
     * @return Result future.
     */
    private IgniteInternalFuture<EmptyResult> prepare(ChangeCacheEncryptionRequest req) {
        if (ctx.clientNode())
            return new GridFinishedFuture<>();

        if (started()) {
            return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected. " +
                "The previous change was not completed."));
        }

        if (ctx.encryption().isMasterKeyChangeInProgress()) {
            return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected. " +
                "The previous master key change was not completed."));
        }

        this.req = req;

        try {
            for (int i = 0; i < req.groupIds().length; i++) {
                int grpId = req.groupIds()[i];
                int keyId = req.keyIds()[i] & 0xff;

                if (!ctx.encryption().reencryptionFuture(grpId).isDone()) {
                    return new GridFinishedFuture<>(
                        new IgniteException("Reencryption is in progress [grpId=" + grpId + "]"));
                }

                Map<Integer, Integer> keysInfo = ctx.encryption().groupKeysInfo(grpId);

                if (keysInfo == null) {
                    return new GridFinishedFuture<>(
                        new IgniteException("Encrypted cache group not found [grpId=" + grpId + "]"));
                }

                for (int locKeyId : keysInfo.keySet()) {
                    if (locKeyId != keyId)
                        continue;

                    Set<Long> walSegments = ctx.encryption().walSegmentsByKey(grpId, keyId);

                    if (walSegments.isEmpty())
                        break;

                    GroupKey currKey = ctx.encryption().groupKey(grpId);

                    return new GridFinishedFuture<>(new IgniteException("Cannot add new key identifier, it's " +
                        "already present. There existing WAL segments that encrypted with this key [" +
                        "grpId=" + grpId + ", newId=" + keyId + ", currId=" + currKey.unsignedId() +
                        ", walSegments=" + walSegments + "]."));
                }
            }
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected [nodeId=" +
                ctx.localNodeId() + ']', e));
        }

        return new GridFinishedFuture<>(new EmptyResult());
    }

    /**
     * Starts group key change if there are no errors.
     *
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void finishPrepare(UUID id, Map<UUID, EmptyResult> res, Map<UUID, Exception> err) {
        if (!err.isEmpty()) {
            if (req != null && req.requestId().equals(id))
                req = null;

            completeFuture(id, err, fut);
        }
        else if (U.isLocalNodeCoordinator(ctx.discovery()))
            performGKChangeProc.start(id, req);
    }

    /**
     * Sets new encrpytion key as active (for writing) and starts background reencryption.
     *
     * @param req Request.
     * @return Result future.
     */
    private IgniteInternalFuture<EmptyResult> perform(ChangeCacheEncryptionRequest req) {
        if (this.req == null || !this.req.equals(req))
            return new GridFinishedFuture<>(new IgniteException("Unknown cache group key change was rejected."));

        try {
            if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
                throw new IgniteException("Cache group key change was rejected. The cluster is inactive.");

            if (!ctx.clientNode())
                ctx.encryption().changeCacheGroupKeyLocal(req);
        } catch (Exception e) {
            return new GridFinishedFuture<>(e);
        } finally {
            this.req = null;
        }

        return new GridFinishedFuture<>(new EmptyResult());
    }

    /**
     * Finishes cache encryption key rotation.
     *
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void finishPerform(UUID id, Map<UUID, EmptyResult> res, Map<UUID, Exception> err) {
        completeFuture(id, err, fut);
    }

    /**
     * @param reqId Request id.
     * @param err Exception.
     * @param fut Key change future.
     * @return {@code True} if future was completed by this call.
     */
    private boolean completeFuture(UUID reqId, Map<UUID, Exception> err, GroupKeyChangeFuture fut) {
        boolean isInitiator = fut != null && fut.id().equals(reqId);

        if (!isInitiator || fut.isDone())
            return false;

        return !F.isEmpty(err) ? fut.onDone(F.firstValue(err)) : fut.onDone();
    }

    /** Cache group key change future. */
    private static class GroupKeyChangeFuture extends KeyChangeFuture {
        /** Request. */
        private final ChangeCacheEncryptionRequest req;

        /**
         * @param req Request.
         */
        GroupKeyChangeFuture(ChangeCacheEncryptionRequest req) {
            super(req.requestId());

            this.req = req;
        }

        /** @return Topology version. */
        public ChangeCacheEncryptionRequest request() {
            return req;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GroupKeyChangeFuture.class, this);
        }
    }
}
