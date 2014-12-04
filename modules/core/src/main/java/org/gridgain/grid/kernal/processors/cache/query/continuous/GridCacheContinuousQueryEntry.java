/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheValueBytes.*;

/**
 * Entry implementation.
 */
@SuppressWarnings("TypeParameterHidesVisibleType")
public class GridCacheContinuousQueryEntry<K, V> implements GridCacheEntry<K, V>, GridCacheDeployable, Externalizable,
    org.gridgain.grid.cache.query.GridCacheContinuousQueryEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache context. */
    @SuppressWarnings("TransientFieldNotInitialized")
    @GridToStringExclude
    private final transient GridCacheContext ctx;

    /** Cache entry. */
    @SuppressWarnings("TransientFieldNotInitialized")
    @GridToStringExclude
    private final transient GridCacheEntry<K, V> impl;

    /** Key. */
    @GridToStringInclude
    private K key;

    /** New value. */
    @GridToStringInclude
    private V newVal;

    /** Old value. */
    @GridToStringInclude
    private V oldVal;

    /** Serialized key. */
    private byte[] keyBytes;

    /** Serialized value. */
    @GridToStringExclude
    private GridCacheValueBytes newValBytes;

    /** Serialized value. */
    @GridToStringExclude
    private GridCacheValueBytes oldValBytes;

    /** Cache name. */
    private String cacheName;

    /** Deployment info. */
    @GridToStringExclude
    private GridDeploymentInfo depInfo;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheContinuousQueryEntry() {
        ctx = null;
        impl = null;
    }

    /**
     * @param ctx Cache context.
     * @param impl Cache entry.
     * @param key Key.
     * @param newVal Value.
     * @param newValBytes Value bytes.
     * @param oldVal Old value.
     * @param oldValBytes Old value bytes.
     */
    GridCacheContinuousQueryEntry(GridCacheContext<K, V> ctx, GridCacheEntry<K, V> impl, K key, @Nullable V newVal,
        @Nullable GridCacheValueBytes newValBytes, @Nullable V oldVal, @Nullable GridCacheValueBytes oldValBytes) {
        assert ctx != null;
        assert impl != null;
        assert key != null;

        this.ctx = ctx;
        this.impl = impl;
        this.key = key;
        this.newVal = newVal;
        this.newValBytes = newValBytes;
        this.oldVal = oldVal;
        this.oldValBytes = oldValBytes;
    }

    /**
     * Unmarshals value from bytes if needed.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @throws GridException In case of error.
     */
    void initValue(GridMarshaller marsh, @Nullable ClassLoader ldr) throws GridException {
        assert marsh != null;

        if (newVal == null && newValBytes != null && !newValBytes.isNull())
            newVal = newValBytes.isPlain() ? (V)newValBytes.get() : marsh.<V>unmarshal(newValBytes.get(), ldr);
    }

    /**
     * @return Cache name.
     */
    String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName New cache name.
     */
    void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @param marsh Marshaller.
     * @throws GridException In case of error.
     */
    void p2pMarshal(GridMarshaller marsh) throws GridException {
        assert marsh != null;

        assert key != null;

        keyBytes = marsh.marshal(key);

        if (newValBytes == null || newValBytes.isNull())
            newValBytes = newVal != null ?
                newVal instanceof byte[] ? plain(newVal) : marshaled(marsh.marshal(newVal)) : null;

        if (oldValBytes == null || oldValBytes.isNull())
            oldValBytes = oldVal != null ?
                oldVal instanceof byte[] ? plain(oldVal) : marshaled(marsh.marshal(oldVal)) : null;
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @throws GridException In case of error.
     */
    void p2pUnmarshal(GridMarshaller marsh, @Nullable ClassLoader ldr) throws GridException {
        assert marsh != null;

        assert key == null : "Key should be null: " + key;
        assert newVal == null : "New value should be null: " + newVal;
        assert oldVal == null : "Old value should be null: " + oldVal;
        assert keyBytes != null;

        key = marsh.unmarshal(keyBytes, ldr);

        if (newValBytes != null && !newValBytes.isNull())
            newVal = newValBytes.isPlain() ? (V)newValBytes.get() : marsh.<V>unmarshal(newValBytes.get(), ldr);

        if (oldValBytes != null && !oldValBytes.isNull())
            oldVal = oldValBytes.isPlain() ? (V)oldValBytes.get() : marsh.<V>unmarshal(oldValBytes.get(), ldr);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection() {
        return impl.projection();
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return newVal;
    }

    /** {@inheritDoc} */
    @Override public V getOldValue() {
        return oldVal;
    }

    /** {@inheritDoc} */
    @Override public V setValue(V val) {
        ctx.denyOnFlag(READ);

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V peek() {
        assert impl != null;

        return newVal;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) throws GridException {
        assert impl != null;

        return impl.peek(modes);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get()
        throws GridException {
        assert impl != null;

        ctx.denyOnFlag(LOCAL);

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> getAsync() {
        assert impl != null;

        ctx.denyOnFlag(LOCAL);

        return new GridFinishedFuture<>(ctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Nullable @Override public V reload() throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> reloadAsync() {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        assert impl != null;

        return impl.isLocked();
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        assert impl != null;

        return impl.isLockedByThread();
    }

    /** {@inheritDoc} */
    @Override public Object version() {
        assert impl != null;

        return impl.version();
    }

    /** {@inheritDoc} */
    @Override public long expirationTime() {
        assert impl != null;

        return impl.expirationTime();
    }

    /** {@inheritDoc} */
    @Override public long timeToLive() {
        assert impl != null;

        return impl.timeToLive();
    }

    /** {@inheritDoc} */
    @Override public void timeToLive(long ttl) {
        assert impl != null;

        impl.timeToLive(ttl);
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        assert impl != null;

        return impl.primary();
    }

    /** {@inheritDoc} */
    @Override public boolean backup() {
        assert impl != null;

        return impl.backup();
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        assert impl != null;

        return impl.partition();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V set(V val, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> setAsync(V val, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setIfAbsent(V val) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> setIfAbsentAsync(V val) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public boolean setx(V val, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> setxAsync(V val,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean setxIfAbsent(@Nullable V val) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> setxIfAbsentAsync(V val) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Override public void transform(IgniteClosure<V, V> transformer) throws GridException {
        ctx.denyOnFlag(READ);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAsync(IgniteClosure<V, V> transformer) {
        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V replace(V val) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> replaceAsync(V val) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(V val) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replacexAsync(V val) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(V oldVal, V newVal) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replaceAsync(V oldVal, V newVal) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(@Nullable GridPredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> removeAsync(@Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public boolean removex(@Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removexAsync(@Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(V val) throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removeAsync(V val) {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return new GridFinishedFuture<>(ctx.kernalContext(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean clear() {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean compact()
        throws GridException {
        assert impl != null;

        ctx.denyOnFlag(READ);

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lock(long timeout, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        assert impl != null;

        return impl.lock(timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> lockAsync(long timeout,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        assert impl != null;

        return impl.lockAsync(timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlock(GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        assert impl != null;

        impl.unlock(filter);
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        assert impl != null;

        return impl.isCached();
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws GridException {
        assert impl != null;

        return impl.memorySize();
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        assert impl != null;

        impl.copyMeta(from);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        assert impl != null;

        impl.copyMeta(data);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V addMeta(String name, V val) {
        assert impl != null;

        return impl.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V putMetaIfAbsent(String name, V val) {
        assert impl != null;

        return impl.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V putMetaIfAbsent(String name, Callable<V> c) {
        assert impl != null;

        return impl.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, V val) {
        assert impl != null;

        return impl.addMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V addMetaIfAbsent(String name, @Nullable Callable<V> c) {
        assert impl != null;

        return impl.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> V meta(String name) {
        assert impl != null;

        return impl.meta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> V removeMeta(String name) {
        assert impl != null;

        return impl.removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean removeMeta(String name, V val) {
        assert impl != null;

        return impl.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> Map<String, V> allMeta() {
        assert impl != null;

        return impl.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        assert impl != null;

        return impl.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean hasMeta(String name, V val) {
        assert impl != null;

        return impl.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean replaceMeta(String name, V curVal, V newVal) {
        assert impl != null;

        return impl.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void prepare(GridDeploymentInfo depInfo) {
        this.depInfo = depInfo;
    }

    /** {@inheritDoc} */
    @Override public GridDeploymentInfo deployInfo() {
        return depInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean b = keyBytes != null;

        out.writeBoolean(b);

        if (b) {
            U.writeByteArray(out, keyBytes);

            if (newValBytes != null && !newValBytes.isNull()) {
                out.writeBoolean(true);
                out.writeBoolean(newValBytes.isPlain());
                U.writeByteArray(out, newValBytes.get());
            }
            else
                out.writeBoolean(false);

            if (oldValBytes != null && !oldValBytes.isNull()) {
                out.writeBoolean(true);
                out.writeBoolean(oldValBytes.isPlain());
                U.writeByteArray(out, oldValBytes.get());
            }
            else
                out.writeBoolean(false);

            U.writeString(out, cacheName);
            out.writeObject(depInfo);
        }
        else {
            out.writeObject(key);
            out.writeObject(newVal);
            out.writeObject(oldVal);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean b = in.readBoolean();

        if (b) {
            keyBytes = U.readByteArray(in);

            if (in.readBoolean())
                newValBytes = in.readBoolean() ? plain(U.readByteArray(in)) : marshaled(U.readByteArray(in));

            if (in.readBoolean())
                oldValBytes = in.readBoolean() ? plain(U.readByteArray(in)) : marshaled(U.readByteArray(in));

            cacheName = U.readString(in);
            depInfo = (GridDeploymentInfo)in.readObject();
        }
        else {
            key = (K)in.readObject();
            newVal = (V)in.readObject();
            oldVal = (V)in.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheContinuousQueryEntry.class, this);
    }
}
