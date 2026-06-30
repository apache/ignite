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
 * Per-message deployer. A generated {@code <Msg>Deployer} implements {@link #prepareDeployment} to deploy the message's
 * fields. The {@code static} methods are the facade through which all non-generated code (custom {@code prepareDeployment}
 * in messages, message-building code) reaches {@link GridCacheMessage}'s package-private deployment helpers — so that, as
 * with marshalling, deployment internals are never touched directly from outside the {@code cache} package.
 */
public interface GridCacheMessageDeployer<M extends GridCacheMessage> {
    /** Prepares deployment info for all deployable fields of {@code msg}. */
    void prepareDeployment(M msg, GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException;

    /** Bridge to {@link GridCacheMessage#prepareObjectDeployment}; no-op when the cache context is absent. */
    static void prepareObject(GridCacheMessage msg, @Nullable Object o, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.prepareObjectDeployment(o, cctx);
    }

    /** Bridge to {@link GridCacheMessage#prepareCacheObjectDeployment}; no-op when the cache context is absent. */
    static void prepareCacheObject(GridCacheMessage msg, @Nullable CacheObject obj, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.prepareCacheObjectDeployment(obj, cctx);
    }

    /** Bridge to {@link GridCacheMessage#prepareCacheObjectsDeployment}; no-op when the cache context is absent. */
    static void prepareCacheObjects(GridCacheMessage msg, @Nullable Collection<? extends CacheObject> col,
        @Nullable GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        if (cctx != null)
            msg.prepareCacheObjectsDeployment(col, cctx);
    }

    /** Bridge to {@link GridCacheMessage#prepareCollectionDeployment}; no-op when the cache context is absent. */
    static void prepareCollection(GridCacheMessage msg, @Nullable Collection<?> col, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.prepareCollectionDeployment(col, cctx);
    }

    /** Bridge to {@link GridCacheMessage#prepareInvokeArgumentsDeployment}; no-op when the cache context is absent. */
    static void prepareInvokeArguments(GridCacheMessage msg, @Nullable Object[] args, @Nullable GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        if (cctx != null)
            msg.prepareInvokeArgumentsDeployment(args, cctx);
    }

    /** Bridge to {@link GridCacheMessage#prepareInfosDeployment}; no-op when the cache context is absent. */
    static void prepareInfos(GridCacheMessage msg, @Nullable Iterable<? extends GridCacheEntryInfo> infos,
        @Nullable GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        if (cctx != null)
            msg.prepareInfosDeployment(infos, cctx.shared(), cctx.cacheObjectContext());
    }

    /** Bridge to {@link GridCacheMessage#prepareInfoDeployment}. */
    static void prepareInfo(GridCacheMessage msg, GridCacheEntryInfo info, GridCacheSharedContext<?, ?> ctx,
        CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        msg.prepareInfoDeployment(info, ctx, cacheObjCtx);
    }

    /** Bridge to {@link GridCacheMessage#prepareTxDeployment} for generated deployers. */
    static void prepareTxEntries(GridCacheMessage msg, @Nullable Iterable<IgniteTxEntry> entries, GridCacheSharedContext<?, ?> ctx)
        throws IgniteCheckedException {
        msg.prepareTxDeployment(entries, ctx);
    }

    /** Forces deployment info on {@code msg} when peer-class-loading is enabled. */
    static void forceDeploymentInfo(GridCacheMessage msg, GridCacheSharedContext<?, ?> ctx) {
        if (!msg.addDepInfo && ctx.deploymentEnabled())
            msg.addDepInfo = true;
    }

    /**
     * Prepares deployment for {@code msg} through its factory-registered deployer (a no-op when {@code msg} is
     * {@code null} — e.g. an absent nested message — or the message has no registered deployer). Single entry point
     * for message deployment: called both by message-sending code and by a generated deployer delegating to a nested
     * message. Mirrors the static {@code MessageMarshaller#marshal}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void prepareDeployment(MessageFactory factory, @Nullable GridCacheMessage msg, GridCacheSharedContext<?, ?> ctx)
        throws IgniteCheckedException {
        if (msg == null)
            return;

        GridCacheMessageDeployer deployer = factory.deployer(msg.directType());

        if (deployer != null)
            deployer.prepareDeployment(msg, ctx);
    }
}
