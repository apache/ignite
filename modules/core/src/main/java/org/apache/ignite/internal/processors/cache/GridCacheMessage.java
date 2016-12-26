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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Parent of all cache messages.
 */
public abstract class GridCacheMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum number of cache lookup indexes. */
    public static final int MAX_CACHE_MSG_LOOKUP_INDEX = 5;

    /** Cache message index field name. */
    public static final String CACHE_MSG_INDEX_FIELD_NAME = "CACHE_MSG_IDX";

    /** Message index id. */
    private static final AtomicInteger msgIdx = new AtomicInteger();

    /** Null message ID. */
    private static final long NULL_MSG_ID = -1;

    /** ID of this message. */
    private long msgId = NULL_MSG_ID;

    /** */
    @GridToStringInclude
    private GridDeploymentInfoBean depInfo;

    /** */
    @GridDirectTransient
    protected boolean addDepInfo;

    /** Force addition of deployment info regardless of {@code addDepInfo} flag value.*/
    @GridDirectTransient
    protected boolean forceAddDepInfo;

    /** */
    @GridDirectTransient
    private IgniteCheckedException err;

    /** */
    @GridDirectTransient
    private boolean skipPrepare;

    /** Cache ID. */
    @GridToStringInclude
    protected int cacheId;

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
    void messageId(long msgId) {
        this.msgId = msgId;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
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
     *  Deployment enabled flag indicates whether deployment info has to be added to this message.
     *
     * @return {@code true} or if deployment info must be added to the the message, {@code false} otherwise.
     */
    public abstract boolean addDeploymentInfo();

    /**
     * @param o Object to prepare for marshalling.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void prepareObject(@Nullable Object o, GridCacheContext ctx) throws IgniteCheckedException {
        assert addDepInfo || forceAddDepInfo;

        if (!skipPrepare && o != null) {
            GridDeploymentInfo d = ctx.deploy().globalDeploymentInfo();

            if (d != null) {
                prepare(d);

                // Global deployment has been injected.
                skipPrepare = true;
            }
            else {
                Class<?> cls = U.detectClass(o);

                ctx.deploy().registerClass(cls);

                ClassLoader ldr = U.detectClassLoader(cls);

                if (ldr instanceof GridDeploymentInfo)
                    prepare((GridDeploymentInfo)ldr);
            }
        }
    }

    /**
     * @param depInfo Deployment to set.
     * @see GridCacheDeployable#prepare(GridDeploymentInfo)
     */
    public final void prepare(GridDeploymentInfo depInfo) {
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
    public GridDeploymentInfo deployInfo() {
        return depInfo;
    }

    /**
     * This method is called before the whole message is serialized
     * and is responsible for pre-marshalling state.
     *
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * This method is called after the message is deserialized and is responsible for
     * unmarshalling state marshalled in {@link #prepareMarshal(GridCacheSharedContext)} method.
     *
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param info Entry to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void marshalInfo(GridCacheEntryInfo info, GridCacheContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (info != null) {
            info.marshal(ctx);

            if (addDepInfo) {
                if (info.key() != null)
                    prepareObject(info.key().value(ctx.cacheObjectContext(), false), ctx);

                CacheObject val = info.value();

                if (val != null) {
                    val.finishUnmarshal(ctx.cacheObjectContext(), ctx.deploy().globalLoader());

                    prepareObject(CU.value(val, ctx, false), ctx);
                }
            }
        }
    }

    /**
     * @param info Entry to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void unmarshalInfo(GridCacheEntryInfo info, GridCacheContext ctx,
        ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (info != null)
            info.unmarshal(ctx, ldr);
    }

    /**
     * @param infos Entries to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void marshalInfos(
        Iterable<? extends GridCacheEntryInfo> infos,
        GridCacheContext ctx
    ) throws IgniteCheckedException {
        assert ctx != null;

        if (infos != null)
            for (GridCacheEntryInfo e : infos)
                marshalInfo(e, ctx);
    }

    /**
     * @param infos Entries to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void unmarshalInfos(Iterable<? extends GridCacheEntryInfo> infos,
        GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (infos != null)
            for (GridCacheEntryInfo e : infos)
                unmarshalInfo(e, ctx, ldr);
    }

    /**
     * @param txEntries Entries to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void marshalTx(Iterable<IgniteTxEntry> txEntries, GridCacheSharedContext ctx)
        throws IgniteCheckedException {
        assert ctx != null;

        if (txEntries != null) {
            boolean transferExpiry = transferExpiryPolicy();
            boolean p2pEnabled = ctx.deploymentEnabled();

            for (IgniteTxEntry e : txEntries) {
                e.marshal(ctx, transferExpiry);

                GridCacheContext cctx = e.context();

                if (addDepInfo) {
                    if (e.key() != null)
                        prepareObject(e.key().value(cctx.cacheObjectContext(), false), cctx);

                    if (e.value() != null)
                        prepareObject(e.value().value(cctx.cacheObjectContext(), false), cctx);

                    if (e.entryProcessors() != null) {
                        for (T2<EntryProcessor<Object, Object, Object>, Object[]> entProc : e.entryProcessors())
                            prepareObject(entProc.get1(), cctx);
                    }
                }
                else if (p2pEnabled && e.entryProcessors() != null) {
                    if (!forceAddDepInfo)
                        forceAddDepInfo = true;

                    for (T2<EntryProcessor<Object, Object, Object>, Object[]> entProc : e.entryProcessors())
                        prepareObject(entProc.get1(), cctx);
                }
            }
        }
    }

    /**
     * @return {@code True} if entries expire policy should be marshalled.
     */
    protected boolean transferExpiryPolicy() {
        return false;
    }

    /**
     * @param txEntries Entries to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void unmarshalTx(Iterable<IgniteTxEntry> txEntries,
        boolean near,
        GridCacheSharedContext ctx,
        ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (txEntries != null) {
            for (IgniteTxEntry e : txEntries)
                e.unmarshal(ctx, near, ldr);
        }
    }

    /**
     * @param args Arguments to marshal.
     * @param ctx Context.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final byte[][] marshalInvokeArguments(@Nullable Object[] args, GridCacheContext ctx)
        throws IgniteCheckedException {
        assert ctx != null;

        if (args == null || args.length == 0)
            return null;

        byte[][] argsBytes = new byte[args.length][];

        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];

            if (addDepInfo)
                prepareObject(arg, ctx);

            argsBytes[i] = arg == null ? null : CU.marshal(ctx, arg);
        }

        return argsBytes;
    }


    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final Object[] unmarshalInvokeArguments(@Nullable byte[][] byteCol,
        GridCacheSharedContext ctx,
        ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        Object[] args = new Object[byteCol.length];

        Marshaller marsh = ctx.marshaller();

        for (int i = 0; i < byteCol.length; i++)
            args[i] = byteCol[i] == null ? null : U.unmarshal(marsh, byteCol[i], U.resolveClassLoader(ldr, ctx.gridConfig()));

        return args;
    }

    /**
     * @param col Collection to marshal.
     * @param ctx Context.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected List<byte[]> marshalCollection(@Nullable Collection<?> col,
        GridCacheContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (col == null)
            return null;

        List<byte[]> byteCol = new ArrayList<>(col.size());

        for (Object o : col) {
            if (addDepInfo)
                prepareObject(o, ctx);

            byteCol.add(o == null ? null : CU.marshal(ctx, o));
        }

        return byteCol;
    }

    /**
     * @param col Collection.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    protected final void prepareMarshalCacheObjects(@Nullable List<? extends CacheObject> col,
        GridCacheContext ctx) throws IgniteCheckedException {
        if (col == null)
            return;

        int size = col.size();

        for (int i = 0 ; i < size; i++)
            prepareMarshalCacheObject(col.get(i), ctx);
    }

    /**
     * @param obj Object.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void prepareMarshalCacheObject(CacheObject obj, GridCacheContext ctx) throws IgniteCheckedException {
        if (obj != null) {
            obj.prepareMarshal(ctx.cacheObjectContext());

            if (addDepInfo)
                prepareObject(obj.value(ctx.cacheObjectContext(), false), ctx);
        }
    }

    /**
     * @param col Collection.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void prepareMarshalCacheObjects(@Nullable Collection<? extends CacheObject> col,
        GridCacheContext ctx) throws IgniteCheckedException {
        if (col == null)
            return;

        for (CacheObject obj : col) {
            if (obj != null) {
                obj.prepareMarshal(ctx.cacheObjectContext());

                if (addDepInfo)
                    prepareObject(obj.value(ctx.cacheObjectContext(), false), ctx);
            }
        }
    }

    /**
     * @param col Collection.
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    protected final void finishUnmarshalCacheObjects(@Nullable List<? extends CacheObject> col,
        GridCacheContext ctx,
        ClassLoader ldr)
        throws IgniteCheckedException
    {
        if (col == null)
            return;

        int size = col.size();

        for (int i = 0 ; i < size; i++) {
            CacheObject obj = col.get(i);

            if (obj != null)
                obj.finishUnmarshal(ctx.cacheObjectContext(), ldr);
        }
    }

    /**
     * @param col Collection.
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void finishUnmarshalCacheObjects(@Nullable Collection<? extends CacheObject> col,
        GridCacheContext ctx,
        ClassLoader ldr)
        throws IgniteCheckedException
    {
        if (col == null)
            return;

        for (CacheObject obj : col) {
            if (obj != null)
                obj.finishUnmarshal(ctx.cacheObjectContext(), ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected <T> List<T> unmarshalCollection(@Nullable Collection<byte[]> byteCol,
        GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        List<T> col = new ArrayList<>(byteCol.size());

        Marshaller marsh = ctx.marshaller();

        for (byte[] bytes : byteCol)
            col.add(bytes == null ? null : U.<T>unmarshal(marsh, bytes, U.resolveClassLoader(ldr, ctx.gridConfig())));

        return col;
    }

    /**
     * @param ctx Context.
     * @return Logger.
     */
    public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.messageLogger();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("depInfo", depInfo))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("msgId", msgId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                depInfo = reader.readMessage("depInfo");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msgId = reader.readLong("msgId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheMessage.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMessage.class, this, "cacheId", cacheId);
    }
}
