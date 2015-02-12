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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.event.*;
import java.io.*;

import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.internal.processors.cache.GridCacheValueBytes.*;

/**
 * Entry implementation.
 */
@SuppressWarnings("TypeParameterHidesVisibleType")
public class GridCacheContinuousQueryEntry<K, V> implements Cache.Entry<K, V>, GridCacheDeployable, Externalizable,
    CacheContinuousQueryEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Event type enum values. */
    private static final EventType[] EVT_TYPE_VALS = EventType.values();

    /** Cache context. */
    @SuppressWarnings("TransientFieldNotInitialized")
    @GridToStringExclude
    private final transient GridCacheContext<K, V> ctx;

    /** Cache entry. */
    @SuppressWarnings("TransientFieldNotInitialized")
    @GridToStringExclude
    private final transient Cache.Entry<K, V> impl;

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

    /** */
    private EventType evtType;

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
     * @param evtType Event type.
     */
    GridCacheContinuousQueryEntry(GridCacheContext<K, V> ctx,
        Cache.Entry<K, V> impl,
        K key,
        @Nullable V newVal,
        @Nullable GridCacheValueBytes newValBytes,
        @Nullable V oldVal,
        @Nullable GridCacheValueBytes oldValBytes,
        EventType evtType) {
        assert ctx != null;
        assert impl != null;
        assert key != null;
        assert evtType != null;

        this.ctx = ctx;
        this.impl = impl;
        this.key = key;
        this.newVal = newVal;
        this.newValBytes = newValBytes;
        this.oldVal = oldVal;
        this.oldValBytes = oldValBytes;
        this.evtType = evtType;
    }

    /**
     * @return Cache entry.
     */
    Cache.Entry<K, V> entry() {
        return impl;
    }

    /**
     * @return Cache context.
     */
    GridCacheContext<K, V> context() {
        return ctx;
    }

    /**
     * @return New value bytes.
     */
    GridCacheValueBytes newValueBytes() {
        return newValBytes;
    }

    /**
     * @return {@code True} if old value is set.
     */
    boolean hasOldValue() {
        return oldVal != null || (oldValBytes != null && !oldValBytes.isNull());
    }

    /**
     * @return {@code True} if entry expired.
     */
    public EventType eventType() {
        return evtType;
    }

    /**
     * Unmarshals value from bytes if needed.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @throws IgniteCheckedException In case of error.
     */
    void initValue(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException In case of error.
     */
    void p2pMarshal(Marshaller marsh) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException In case of error.
     */
    void p2pUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
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
    @Override public void prepare(GridDeploymentInfo depInfo) {
        this.depInfo = depInfo;
    }

    /** {@inheritDoc} */
    @Override public GridDeploymentInfo deployInfo() {
        return depInfo;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if(clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
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

        out.writeByte((byte)evtType.ordinal());
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

        evtType = EVT_TYPE_VALS[in.readByte()];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheContinuousQueryEntry.class, this);
    }
}
