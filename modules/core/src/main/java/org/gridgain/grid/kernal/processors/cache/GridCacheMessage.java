/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Parent of all cache messages.
 */
public abstract class GridCacheMessage<K, V> extends GridTcpCommunicationMessageAdapter {
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
    protected final void prepareFilter(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filters,
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
    protected final void marshalInfos(Iterable<? extends GridCacheEntryInfo<K, V>> infos, GridCacheSharedContext<K, V> ctx)
        throws IgniteCheckedException {
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
    protected final void marshalTx(Iterable<GridCacheTxEntry<K, V>> txEntries, GridCacheSharedContext<K, V> ctx)
        throws IgniteCheckedException {
        assert ctx != null;

        if (txEntries != null) {
            boolean transferExpiry = transferExpiryPolicy();

            for (GridCacheTxEntry<K, V> e : txEntries) {
                if (transferExpiry)
                    e.transferExpiryPolicyIfNeeded();

                e.marshal(ctx);

                if (ctx.deploymentEnabled()) {
                    prepareObject(e.key(), ctx);
                    prepareObject(e.value(), ctx);
                    prepareFilter(e.filters(), ctx);
                }
            }
        }
    }

    protected boolean transferExpiryPolicy() {
        return false;
    }

    /**
     * @param txEntries Entries to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    protected final void unmarshalTx(Iterable<GridCacheTxEntry<K, V>> txEntries, boolean near,
        GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (txEntries != null) {
            for (GridCacheTxEntry<K, V> e : txEntries)
                e.unmarshal(ctx, near, ldr);
        }
    }

    /**
     * @param filter Collection to marshal.
     * @param ctx Context.
     * @return Marshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected final <T> byte[][] marshalFilter(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (filter == null)
            return null;

        byte[][] filterBytes = new byte[filter.length][];

        for (int i = 0; i < filter.length; i++) {
            IgnitePredicate<GridCacheEntry<K, V>> p = filter[i];

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
    @Nullable protected final <T> IgnitePredicate<GridCacheEntry<K, V>>[] unmarshalFilter(
        @Nullable byte[][] byteCol, GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        IgnitePredicate<GridCacheEntry<K, V>>[] filter = new IgnitePredicate[byteCol.length];

        IgniteMarshaller marsh = ctx.marshaller();

        for (int i = 0; i < byteCol.length; i++)
            filter[i] = byteCol[i] == null ? null :
                marsh.<IgnitePredicate<GridCacheEntry<K, V>>>unmarshal(byteCol[i], ldr);

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

        IgniteMarshaller marsh = ctx.marshaller();

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

        IgniteMarshaller marsh = ctx.marshaller();

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

        IgniteMarshaller marsh = ctx.marshaller();

        for (Map.Entry<byte[], Boolean> e : byteMap.entrySet())
            map.put(marsh.<K1>unmarshal(e.getKey(), ldr), e.getValue());

        return map;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
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
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putInt(cacheId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putMessage(depInfo))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putLong(msgId))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                if (buf.remaining() < 4)
                    return false;

                cacheId = commState.getInt();

                commState.idx++;

            case 1:
                Object depInfo0 = commState.getMessage();

                if (depInfo0 == MSG_NOT_READ)
                    return false;

                depInfo = (GridDeploymentInfoBean)depInfo0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 8)
                    return false;

                msgId = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMessage.class, this);
    }
}
