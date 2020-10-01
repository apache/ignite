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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager.EmptyResult;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager.KeyChangeFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;

import static org.apache.ignite.internal.IgniteFeatures.CACHE_GROUP_KEY_CHANGE;
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

    /** Group encryption keys. */
    private final CacheGroupEncryptionKeys keys;

    /** Cache group key change future. */
    private volatile GroupKeyChangeFuture fut;

    /** Cache group key change request. */
    private volatile ChangeCacheEncryptionRequest req;

    /**
     * @param ctx Grid kernal context.
     */
    GroupKeyChangeProcess(GridKernalContext ctx, CacheGroupEncryptionKeys keys) {
        this.ctx = ctx;
        this.keys = keys;

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
     * @param msg Error message.
     */
    public void cancel(String msg) {
        GridFutureAdapter<Void> keyChangeFut = fut;

        if (keyChangeFut != null && !keyChangeFut.isDone())
            keyChangeFut.onDone(new IgniteFutureCancelledException(msg));
    }

    /**
     * Starts cache group encryption key change process.
     *
     * @param cacheOrGrpNames Cache or group names.
     */
    public IgniteFuture<Void> start(Collection<String> cacheOrGrpNames) {
        if (ctx.clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        if (!IgniteFeatures.allNodesSupports(ctx.grid().cluster().nodes(), CACHE_GROUP_KEY_CHANGE))
            throw new IllegalStateException("Not all nodes in the cluster support this operation.");

        if (!ctx.state().clusterState().state().active())
            throw new IgniteException("Operation was rejected. The cluster is inactive.");

        IgniteInternalFuture<Void> fut0 = fut;

        if (fut0 != null && !fut0.isDone()) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Cache group key change was rejected. " +
                "The previous change was not completed."));
        }

        int[] grpIds = new int[cacheOrGrpNames.size()];
        byte[] keyIds = new byte[grpIds.length];

        int n = 0;

        for (String cacheOrGroupName : cacheOrGrpNames) {
            CacheGroupContext grp = ctx.cache().cacheGroup(CU.cacheId(cacheOrGroupName));

            if (grp == null) {
                IgniteInternalCache<?, ?> cache = ctx.cache().cache(cacheOrGroupName);

                if (cache == null) {
                    throw new IgniteException("Cache group key change was rejected. " +
                        "Cache or group \"" + cacheOrGroupName + "\" doesn't exists");
                }

                grp = cache.context().group();

                if (grp.sharedGroup()) {
                    throw new IgniteException("Cache group key change was rejected. " +
                        "Cache or group \"" + cacheOrGroupName + "\" is a part of group " +
                        grp.name() + ". Provide group name instead of cache name for shared groups.");
                }
            }

            if (!grp.config().isEncryptionEnabled()) {
                throw new IgniteException("Cache group key change was rejected. " +
                    "Cache or group \"" + cacheOrGroupName + "\" is not encrypted.");
            }

            if (ctx.encryption().isReencryptionInProgress(grp.groupId())) {
                throw new IgniteException("Cache group key change was rejected. " +
                    "Cache group reencryption is in progress [grp=" + cacheOrGroupName + "]");
            }

            grpIds[n] = grp.groupId();
            keyIds[n] = (byte)(ctx.encryption().groupKey(grp.groupId()).unsignedId() + 1);

            n += 1;
        }

        byte[][] keys = ctx.encryption().createKeys(grpIds.length).get1().toArray(new byte[grpIds.length][]);

        ChangeCacheEncryptionRequest req =
            new ChangeCacheEncryptionRequest(grpIds, keys, keyIds, ctx.config().getEncryptionSpi().getMasterKeyName());

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

        this.req = req;

        try {
            for (int i = 0; i < req.groupIds().length; i++) {
                int grpId = req.groupIds()[i];
                int keyId = req.keyIds()[i] & 0xff;

                if (ctx.encryption().isReencryptionInProgress(grpId)) {
                    return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected. " +
                            "Cache group reencryption is in progress [grpId=" + grpId + "]"));
                }

                List<Integer> keyIds = ctx.encryption().groupKeyIds(grpId);

                if (keyIds == null) {
                    return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected." +
                            "Encrypted cache group not found [grpId=" + grpId + "]"));
                }

                GroupKey currKey = ctx.encryption().groupKey(grpId);

                for (int locKeyId : keyIds) {
                    if (locKeyId != keyId)
                        continue;

                    Long walSegment = keys.reservedSegment(grpId, keyId);

                    // Can overwrite inactive key if it was added during prepare phase.
                    if (walSegment == null && currKey.id() != (byte)keyId)
                        continue;

                    return new GridFinishedFuture<>(
                        new IgniteException("Cache group key change was rejected. Cannot add new key identifier," +
                        "it's  already present. There existing WAL segments that encrypted with this key [" +
                        "grpId=" + grpId + ", newId=" + keyId + ", currId=" + currKey.unsignedId() +
                        ", walSegment=" + walSegment + "]."));
                }
            }

            return ctx.encryption().withMasterKeyChangeReadLock(() -> {
                String curMasterKeyName = ctx.config().getEncryptionSpi().getMasterKeyName();

                if (!curMasterKeyName.equals(req.masterKeyName())) {
                    return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected. " +
                        "Master key has been changed."));
                }

                for (int i = 0; i < req.groupIds().length; i++) {
                    // Store new key as inactive.
                    GroupKeyEncrypted grpKey = new GroupKeyEncrypted(req.keyIds()[i] & 0xff, req.keys()[i]);

                    ctx.encryption().addGroupKey(req.groupIds()[i], grpKey);
                }

                return new GridFinishedFuture<>(new EmptyResult());
            });

        }
        catch (Exception e) {
            return new GridFinishedFuture<>(new IgniteException("Cache group key change was rejected [nodeId=" +
                ctx.localNodeId() + ']', e));
        }
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
            if (!ctx.state().clusterState().state().active())
                throw new IgniteException("Cache group key change was rejected. The cluster is inactive.");

            if (!ctx.clientNode())
                ctx.encryption().changeCacheGroupKeyLocal(req.groupIds(), req.keyIds(), req.keys());
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
