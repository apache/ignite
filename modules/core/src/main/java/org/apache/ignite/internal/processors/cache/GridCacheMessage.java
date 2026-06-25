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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Parent of all cache messages.
 */
public abstract class GridCacheMessage implements Message {
    /** Maximum number of cache lookup indexes. */
    public static final int MAX_CACHE_MSG_LOOKUP_INDEX = 7;

    /** Cache message index field name. */
    public static final String CACHE_MSG_INDEX_FIELD_NAME = "CACHE_MSG_IDX";

    /** Message index id. */
    private static final AtomicInteger msgIdx = new AtomicInteger();

    /** Null message ID. */
    private static final long NULL_MSG_ID = -1;

    /** ID of this message. */
    @Order(0)
    public long msgId = NULL_MSG_ID;

    /** */
    @GridToStringInclude
    @Order(1)
    public GridDeploymentInfoBean depInfo;

    /** */
    @GridToStringInclude
    @Order(2)
    @Nullable public AffinityTopologyVersion lastAffChangedTopVer;

    /** */
    protected boolean addDepInfo;

    /** Force addition of deployment info regardless of {@code addDepInfo} flag value.*/
    protected boolean forceAddDepInfo;

    /** */
    private IgniteCheckedException err;

    /** */
    private boolean skipPrepare;

    /**
     * @return Error, if any.
     */
    @Nullable public Throwable error() {
        return null;
    }

    /**
     * Gets next ID for indexed message ID.
     *
     * @return Message ID.
     */
    public static int nextIndexId() {
        return msgIdx.getAndIncrement();
    }

    /**
     * @return {@code True} if this message is partition exchange message.
     */
    public boolean partitionExchangeMessage() {
        return false;
    }

    /**
     * @return {@code True} if class loading errors should be ignored, false otherwise.
     */
    public boolean ignoreClassErrors() {
        return false;
    }

    /**
     * Gets message lookup index. All messages that does not return -1 in this method must return a unique
     * number in range from 0 to {@link #MAX_CACHE_MSG_LOOKUP_INDEX}.
     *
     * @return Message lookup index.
     */
    public int lookupIndex() {
        return -1;
    }

    /**
     * @return Partition ID this message is targeted to or {@code -1} if it cannot be determined.
     */
    public int partition() {
        return -1;
    }

    /**
     * If class loading error occurred during unmarshalling and {@link #ignoreClassErrors()} is
     * set to {@code true}, then the error will be passed into this method.
     *
     * @param err Error.
     */
    public void onClassError(IgniteCheckedException err) {
        this.err = err;
    }

    /**
     * @return Error set via {@link #onClassError(IgniteCheckedException)} method.
     */
    public IgniteCheckedException classError() {
        return err;
    }

    /**
     * @return Message ID.
     */
    public long messageId() {
        return msgId;
    }

    /**
     * Sets message ID. This method is package protected and is only called
     * by {@link GridCacheIoManager}.
     *
     * @param msgId New message ID.
     */
    public void messageId(long msgId) {
        this.msgId = msgId;
    }

    /**
     * Gets topology version or -1 in case of topology version is not required for this message.
     *
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return AffinityTopologyVersion.NONE;
    }

    /**
     * Returns the earliest affinity topology version for which this message is valid.
     *
     * @return Last affinity topology version when affinity was modified.
     */
    public AffinityTopologyVersion lastAffinityChangedTopologyVersion() {
        if (lastAffChangedTopVer == null || lastAffChangedTopVer.topologyVersion() <= 0)
            return topologyVersion();

        return lastAffChangedTopVer;
    }

    /**
     * Sets the earliest affinity topology version for which this message is valid.
     *
     * @param topVer Last affinity topology version when affinity was modified.
     */
    public void lastAffinityChangedTopologyVersion(AffinityTopologyVersion topVer) {
        lastAffChangedTopVer = topVer;
    }

    /**
     * Deployment enabled flag indicates whether deployment info has to be added to this message.
     *
     * @return {@code true} or if deployment info must be added to the message, {@code false} otherwise.
     */
    public abstract boolean addDeploymentInfo();

    /**
     * @param o Object to prepare for marshalling.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareObjectDeployment(@Nullable Object o, GridCacheContext ctx) throws IgniteCheckedException {
        prepareObjectDeployment(o, ctx.shared());
    }

    /**
     * @param o Object to prepare for marshalling.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareObjectDeployment(@Nullable Object o, GridCacheSharedContext ctx) throws IgniteCheckedException {
        assert addDepInfo || forceAddDepInfo;

        if (!skipPrepare && o != null) {
            GridDeploymentInfo d = ctx.deploy().globalDeploymentInfo();

            if (d != null) {
                prepareDeployment(d);

                // Global deployment has been injected.
                skipPrepare = true;
            }
            else {
                Class<?> cls = U.detectClass(o);

                ctx.deploy().registerClass(cls);

                ClassLoader ldr = U.detectClassLoader(cls);

                if (ldr instanceof GridDeploymentInfo)
                    prepareDeployment((GridDeploymentInfo)ldr);
            }
        }
    }

    /**
     * @param depInfo Deployment to set.
     * @see GridCacheDeployable#prepareDeployment(GridDeploymentInfo)
     */
    public final void prepareDeployment(GridDeploymentInfo depInfo) {
        if (depInfo != this.depInfo) {
            if (this.depInfo != null && depInfo instanceof GridDeployment)
                // Make sure not to replace remote deployment with local.
                if (((GridDeployment)depInfo).local())
                    return;

            this.depInfo = depInfo instanceof GridDeploymentInfoBean ?
                (GridDeploymentInfoBean)depInfo : new GridDeploymentInfoBean(depInfo);
        }
    }

    /**
     * @return Preset deployment info.
     * @see GridCacheDeployable#deployInfo()
     */
    public GridDeploymentInfoBean deployInfo() {
        return depInfo;
    }

    /**
     * @param info Entry to marshal.
     * @param ctx Context.
     * @param cacheObjCtx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareInfoDeployment(GridCacheEntryInfo info,
        GridCacheSharedContext ctx,
        CacheObjectContext cacheObjCtx
    ) throws IgniteCheckedException {
        assert ctx != null;

        if (info != null) {
            if (addDepInfo) {
                if (info.key() != null)
                    prepareObjectDeployment(info.key().value(cacheObjCtx, false), ctx);

                CacheObject val = info.value();

                if (val != null) {
                    val.finishUnmarshal(cacheObjCtx, ctx.deploy().globalLoader());

                    prepareObjectDeployment(val.value(cacheObjCtx, false), ctx);
                }
            }
        }
    }

    /**
     * @param infos Entries to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareInfosDeployment(Iterable<? extends GridCacheEntryInfo> infos, GridCacheSharedContext ctx,
        CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        assert ctx != null;

        if (infos != null)
            for (GridCacheEntryInfo e : infos)
                prepareInfoDeployment(e, ctx, cacheObjCtx);
    }

    /**
     * @param txEntries Entries to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareTxDeployment(Iterable<IgniteTxEntry> txEntries, GridCacheSharedContext ctx)
        throws IgniteCheckedException {
        assert ctx != null;

        if (txEntries != null) {
            boolean p2pEnabled = ctx.deploymentEnabled();

            for (IgniteTxEntry e : txEntries) {
                GridCacheContext cctx = e.context();

                if (addDepInfo) {
                    if (e.key() != null)
                        prepareObjectDeployment(e.key().value(cctx.cacheObjectContext(), false), ctx);

                    if (e.value() != null)
                        prepareObjectDeployment(e.value().value(cctx.cacheObjectContext(), false), ctx);

                    if (e.entryProcessors() != null) {
                        for (T2<EntryProcessor<Object, Object, Object>, Object[]> entProc : e.entryProcessors())
                            prepareObjectDeployment(entProc.get1(), ctx);
                    }
                }
                else if (p2pEnabled && e.entryProcessors() != null) {
                    if (!forceAddDepInfo)
                        forceAddDepInfo = true;

                    for (T2<EntryProcessor<Object, Object, Object>, Object[]> entProc : e.entryProcessors())
                        prepareObjectDeployment(entProc.get1(), ctx);
                }
            }
        }
    }

    /**
     * @param args Arguments to marshal.
     * @param marsh Marshaller.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final byte[][] marshallInvokeArguments(@Nullable Object[] args, Marshaller marsh)
        throws IgniteCheckedException {

        if (args == null || args.length == 0)
            return null;

        byte[][] argsBytes = new byte[args.length][];

        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];

            argsBytes[i] = arg == null ? null : U.marshal(marsh, arg);
        }

        return argsBytes;
    }

    /**
     * @param args Arguments to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareInvokeArgumentsDeployment(@Nullable Object[] args, GridCacheContext ctx)
        throws IgniteCheckedException {
        assert ctx != null;

        if (args == null)
            return;

        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];

            if (addDepInfo)
                prepareObjectDeployment(arg, ctx.shared());
        }
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param marsh Marshaller.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final Object[] unmarshalInvokeArguments(@Nullable byte[][] byteCol,
        Marshaller marsh,
        ClassLoader ldr) throws IgniteCheckedException {
        if (byteCol == null)
            return null;

        Object[] args = new Object[byteCol.length];

        for (int i = 0; i < byteCol.length; i++)
            args[i] = byteCol[i] == null ? null : U.unmarshal(marsh, byteCol[i], ldr);

        return args;
    }

    /**
     * @param col Collection to marshal.
     * @param marsh Marshaller.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected List<byte[]> marshallCollection(@Nullable Collection<?> col, Marshaller marsh) throws IgniteCheckedException {
        if (col == null)
            return null;

        List<byte[]> byteCol = new ArrayList<>(col.size());

        for (Object o : col)
            byteCol.add(o == null ? null : U.marshal(marsh, o));

        return byteCol;
    }

    /**
     * Prepares each element of {@code col} for deployment, calling {@link #prepareObjectDeployment} on each item.
     *
     * @param col Collection to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareCollectionDeployment(@Nullable Collection<?> col,
        GridCacheContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        for (Object o : col) {
            if (addDepInfo)
                prepareObjectDeployment(o, ctx.shared());
        }
    }

    /**
     * @param obj Object.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareCacheObjectDeployment(CacheObject obj, GridCacheContext ctx) throws IgniteCheckedException {
        if (obj != null) {
            if (addDepInfo)
                prepareObjectDeployment(obj.value(ctx.cacheObjectContext(), false), ctx.shared());
        }
    }

    /**
     * @param col Collection.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    final void prepareCacheObjectsDeployment(@Nullable Collection<? extends CacheObject> col,
        GridCacheContext ctx) throws IgniteCheckedException {
        if (col == null)
            return;

        for (CacheObject obj : col) {
            if (obj != null) {
                if (addDepInfo)
                    prepareObjectDeployment(obj.value(ctx.cacheObjectContext(), false), ctx.shared());
            }
        }
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param marsh Marshaller.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected <T> List<T> unmarshalCollection(@Nullable Collection<byte[]> byteCol,
        Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (byteCol == null)
            return null;

        List<T> col = new ArrayList<>(byteCol.size());

        for (byte[] bytes : byteCol)
            col.add(bytes == null ? null : U.unmarshal(marsh, bytes, ldr));

        return col;
    }

    /**
     * @param ctx Context.
     * @return Logger.
     */
    public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.messageLogger();
    }

    /**
     * @param str Bulder.
     * @param name Flag name.
     */
    protected final void appendFlag(StringBuilder str, String name) {
        if (str.length() > 0)
            str.append('|');

        str.append(name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMessage.class, this);
    }
}
