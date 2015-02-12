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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Parent of all cache messages.
 */
public abstract class GridCacheMessage<K, V> extends MessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum number of cache lookup indexes. */
    public static final int MAX_CACHE_MSG_LOOKUP_INDEX = 256;

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
    private Exception err;

    /** */
    @GridDirectTransient
    private boolean skipPrepare;

    /** Cache ID. */
    protected int cacheId;

    /**
     * Gets next ID for indexed message ID.
     *
     * @return Message ID.
     */
    public static int nextIndexId() {
        return msgIdx.getAndIncrement();
    }

    /**
     * @return {@code True} if this message is preloader message.
     */
    public boolean allowForStartup() {
        return false;
    }

    /**
     * @return If this is a transactional message.
     */
    public boolean transactional() {
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
     * If class loading error occurred during unmarshalling and {@link #ignoreClassErrors()} is
     * set to {@code true}, then the error will be passed into this method.
     *
     * @param err Error.
     */
    public void onClassError(Exception err) {
        this.err = err;
    }

    /**
     * @return Error set via {@link #onClassError(Exception)} method.
     */
    public Exception classError() {
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
    public long topologyVersion() {
        return -1;
    }

    /**
     * @param filters Predicate filters.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void prepareFilter(@Nullable IgnitePredicate<Cache.Entry<K, V>>[] filters,
        GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        if (filters != null)
            for (IgnitePredicate filter : filters)
                prepareObject(filter, ctx);
    }

    /**
     * @param o Object to prepare for marshalling.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void prepareObject(@Nullable Object o, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
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
     * @param col Collection of objects to prepare for marshalling.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void prepareObjects(@Nullable Iterable<?> col, GridCacheSharedContext<K, V> ctx)
        throws IgniteCheckedException {
        if (col != null)
            for (Object o : col)
                prepareObject(o, ctx);
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
    public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
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
    public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param info Entry to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void marshalInfo(GridCacheEntryInfo<K, V> info, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (info != null) {
            info.marshal(ctx);

            if (ctx.deploymentEnabled()) {
                prepareObject(info.key(), ctx);
                prepareObject(info.value(), ctx);
            }
        }
    }

    /**
     * @param info Entry to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void unmarshalInfo(GridCacheEntryInfo<K, V> info, GridCacheContext<K, V> ctx,
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
        Iterable<? extends GridCacheEntryInfo<K, V>> infos,
        GridCacheSharedContext<K, V> ctx
    ) throws IgniteCheckedException {
        assert ctx != null;

        if (infos != null)
            for (GridCacheEntryInfo<K, V> e : infos)
                marshalInfo(e, ctx);
    }

    /**
     * @param infos Entries to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void unmarshalInfos(Iterable<? extends GridCacheEntryInfo<K, V>> infos,
        GridCacheContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (infos != null)
            for (GridCacheEntryInfo<K, V> e : infos)
                unmarshalInfo(e, ctx, ldr);
    }

    /**
     * @param txEntries Entries to marshal.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    protected final void marshalTx(Iterable<IgniteTxEntry<K, V>> txEntries, GridCacheSharedContext<K, V> ctx)
        throws IgniteCheckedException {
        assert ctx != null;

        if (txEntries != null) {
            boolean transferExpiry = transferExpiryPolicy();

            for (IgniteTxEntry<K, V> e : txEntries) {
                e.marshal(ctx, transferExpiry);

                if (ctx.deploymentEnabled()) {
                    prepareObject(e.key(), ctx);
                    prepareObject(e.value(), ctx);
                    prepareFilter(e.filters(), ctx);
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
    protected final void unmarshalTx(Iterable<IgniteTxEntry<K, V>> txEntries,
        boolean near,
        GridCacheSharedContext<K, V> ctx,
        ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (txEntries != null) {
            for (IgniteTxEntry<K, V> e : txEntries)
                e.unmarshal(ctx, near, ldr);
        }
    }

    /**
     * @param args Arguments to marshal.
     * @param ctx Context.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final byte[][] marshalInvokeArguments(@Nullable Object[] args,
        GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (args == null || args.length == 0)
            return null;

        byte[][] argsBytes = new byte[args.length][];

        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];

            if (ctx.deploymentEnabled())
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
        GridCacheSharedContext<K, V> ctx,
        ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        Object[] args = new Object[byteCol.length];

        Marshaller marsh = ctx.marshaller();

        for (int i = 0; i < byteCol.length; i++)
            args[i] = byteCol[i] == null ? null : marsh.unmarshal(byteCol[i], ldr);

        return args;
    }

    /**
     * @param filter Collection to marshal.
     * @param ctx Context.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final <T> byte[][] marshalFilter(@Nullable IgnitePredicate<Cache.Entry<K, V>>[] filter,
        GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (filter == null)
            return null;

        byte[][] filterBytes = new byte[filter.length][];

        for (int i = 0; i < filter.length; i++) {
            IgnitePredicate<Cache.Entry<K, V>> p = filter[i];

            if (ctx.deploymentEnabled())
                prepareObject(p, ctx);

            filterBytes[i] = p == null ? null : CU.marshal(ctx, p);
        }

        return filterBytes;
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable protected final <T> IgnitePredicate<Cache.Entry<K, V>>[] unmarshalFilter(
        @Nullable byte[][] byteCol, GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        IgnitePredicate<Cache.Entry<K, V>>[] filter = new IgnitePredicate[byteCol.length];

        Marshaller marsh = ctx.marshaller();

        for (int i = 0; i < byteCol.length; i++)
            filter[i] = byteCol[i] == null ? null :
                marsh.<IgnitePredicate<Cache.Entry<K, V>>>unmarshal(byteCol[i], ldr);

        return filter;
    }

    /**
     * @param col Values collection to marshal.
     * @param ctx Context.
     * @return Marshaled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected List<GridCacheValueBytes> marshalValuesCollection(@Nullable Collection<?> col,
        GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (col == null)
            return null;

        List<GridCacheValueBytes> byteCol = new ArrayList<>(col.size());

        for (Object o : col) {
            if (ctx.deploymentEnabled())
                prepareObject(o, ctx);

            byteCol.add(o == null ? null : o instanceof byte[] ? GridCacheValueBytes.plain(o) :
                GridCacheValueBytes.marshaled(CU.marshal(ctx, o)));
        }

        return byteCol;
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected <T> List<T> unmarshalValueBytesCollection(@Nullable Collection<GridCacheValueBytes> byteCol,
        GridCacheSharedContext<K, V> ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        List<T> col = new ArrayList<>(byteCol.size());

        Marshaller marsh = ctx.marshaller();

        for (GridCacheValueBytes item : byteCol) {
            assert item == null || item.get() != null;

            col.add(item != null ? item.isPlain() ? (T)item.get() : marsh.<T>unmarshal(item.get(), ldr) : null);
        }

        return col;
    }

    /**
     * @param col Collection to marshal.
     * @param ctx Context.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected List<byte[]> marshalCollection(@Nullable Collection<?> col,
        GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (col == null)
            return null;

        List<byte[]> byteCol = new ArrayList<>(col.size());

        for (Object o : col) {
            if (ctx.deploymentEnabled())
                prepareObject(o, ctx);

            byteCol.add(o == null ? null : CU.marshal(ctx, o));
        }

        return byteCol;
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected <T> List<T> unmarshalCollection(@Nullable Collection<byte[]> byteCol,
        GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        List<T> col = new ArrayList<>(byteCol.size());

        Marshaller marsh = ctx.marshaller();

        for (byte[] bytes : byteCol)
            col.add(bytes == null ? null : marsh.<T>unmarshal(bytes, ldr));

        return col;
    }

    /**
     * @param map Map to marshal.
     * @param ctx Context.
     * @return Marshalled map.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("TypeMayBeWeakened") // Don't weaken type to clearly see that it's linked hash map.
    @Nullable protected final LinkedHashMap<byte[], Boolean> marshalBooleanLinkedMap(
        @Nullable LinkedHashMap<?, Boolean> map, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (map == null)
            return null;

        LinkedHashMap<byte[], Boolean> byteMap = U.newLinkedHashMap(map.size());

        for (Map.Entry<?, Boolean> e : map.entrySet()) {
            if (ctx.deploymentEnabled())
                prepareObject(e.getKey(), ctx);

            byteMap.put(CU.marshal(ctx, e.getKey()), e.getValue());
        }

        return byteMap;
    }

    /**
     * @param byteMap Map to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled map.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final <K1> LinkedHashMap<K1, Boolean> unmarshalBooleanLinkedMap(
        @Nullable Map<byte[], Boolean> byteMap, GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteMap == null)
            return null;

        LinkedHashMap<K1, Boolean> map = U.newLinkedHashMap(byteMap.size());

        Marshaller marsh = ctx.marshaller();

        for (Map.Entry<byte[], Boolean> e : byteMap.entrySet())
            map.put(marsh.<K1>unmarshal(e.getKey(), ldr), e.getValue());

        return map;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridCacheMessage _clone = (GridCacheMessage)_msg;

        _clone.msgId = msgId;
        _clone.depInfo = depInfo != null ? (GridDeploymentInfoBean)depInfo.clone() : null;
        _clone.err = err;
        _clone.skipPrepare = skipPrepare;
        _clone.cacheId = cacheId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 0:
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                state++;

            case 1:
                if (!writer.writeMessage("depInfo", depInfo))
                    return false;

                state++;

            case 2:
                if (!writer.writeLong("msgId", msgId))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (state) {
            case 0:
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 1:
                depInfo = reader.readMessage("depInfo");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 2:
                msgId = reader.readLong("msgId");

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMessage.class, this);
    }
}
