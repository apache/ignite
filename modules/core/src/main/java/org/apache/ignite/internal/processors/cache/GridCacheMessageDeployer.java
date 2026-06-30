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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Per-message deployer. A generated {@code <Msg>Deployer} implements {@link #deploy} to deploy the message's
 * fields. The {@code static} methods are the facade through which all non-generated code (custom {@code deploy}
 * in messages, message-building code) reaches {@link GridCacheMessage}'s package-private deployment helpers — so that, as
 * with marshalling, deployment internals are never touched directly from outside the {@code cache} package.
 */
public interface GridCacheMessageDeployer<M extends GridCacheMessage> {
    /** Prepares deployment info for all deployable fields of {@code msg}. */
    void deploy(M msg, GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException;

    /** Bridge to {@link GridCacheMessage#deployObject}; no-op when the cache context is absent. */
    static void deployObject(GridCacheMessage msg, @Nullable Object o, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.deployObject(o, cctx);
    }

    /** Bridge to {@link GridCacheMessage#deployCacheObject}; no-op when the cache context is absent. */
    static void deployCacheObject(GridCacheMessage msg, @Nullable CacheObject obj, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.deployCacheObject(obj, cctx);
    }

    /** Bridge to {@link GridCacheMessage#deployCacheObjects}; no-op when the cache context is absent. */
    static void deployCacheObjects(GridCacheMessage msg, @Nullable Collection<? extends CacheObject> col,
        @Nullable GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        if (cctx != null)
            msg.deployCacheObjects(col, cctx);
    }

    /** Bridge to {@link GridCacheMessage#deployCollection}; no-op when the cache context is absent. */
    static void deployCollection(GridCacheMessage msg, @Nullable Collection<?> col, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.deployCollection(col, cctx);
    }

    /** Bridge to {@link GridCacheMessage#deployInvokeArguments}; no-op when the cache context is absent. */
    static void deployInvokeArguments(GridCacheMessage msg, @Nullable Object[] args, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.deployInvokeArguments(args, cctx);
    }

    /** Bridge to {@link GridCacheMessage#deployInfos}; no-op when the cache context is absent. */
    static void deployInfos(GridCacheMessage msg, @Nullable Iterable<? extends GridCacheEntryInfo> infos,
        @Nullable GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        if (cctx != null)
            msg.deployInfos(infos, cctx.shared(), cctx.cacheObjectContext());
    }

    /** Bridge to {@link GridCacheMessage#deployInfo}. */
    static void deployInfo(GridCacheMessage msg, GridCacheEntryInfo info, GridCacheSharedContext<?, ?> ctx,
        CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        msg.deployInfo(info, ctx, cacheObjCtx);
    }

    /** Bridge to {@link GridCacheMessage#deployTx} for generated deployers. */
    static void deployTxEntries(GridCacheMessage msg, @Nullable Iterable<IgniteTxEntry> entries, GridCacheSharedContext<?, ?> ctx)
        throws IgniteCheckedException {
        msg.deployTx(entries, ctx);
    }

    /** Forces deployment info on {@code msg} when peer-class-loading is enabled. */
    static void forceDeploymentInfo(GridCacheMessage msg, GridCacheSharedContext<?, ?> ctx) {
        if (!msg.addDepInfo && ctx.deploymentEnabled())
            msg.addDepInfo = true;
    }

    /**
     * Deploys {@code msg} through its factory-registered deployer (a no-op when {@code msg} is
     * {@code null} — e.g. an absent nested message — or the message has no registered deployer). Single entry point
     * for message deployment: called both by message-sending code and by a generated deployer delegating to a nested
     * message. Mirrors the static {@code MessageMarshaller#marshal}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void deploy(MessageFactory factory, @Nullable GridCacheMessage msg, GridCacheSharedContext<?, ?> ctx)
        throws IgniteCheckedException {
        if (msg == null)
            return;

        GridCacheMessageDeployer deployer = factory.deployer(msg.directType());

        if (deployer != null)
            deployer.deploy(msg, ctx);
    }
}
