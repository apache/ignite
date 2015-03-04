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
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.apache.ignite.internal.processors.cache.GridCacheValueBytes.*;

/**
 * Continuous query entry.
 */
class CacheContinuousQueryEntry<K, V> implements GridCacheDeployable, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

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
    @GridToStringExclude
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

    public CacheContinuousQueryEntry() {
        // No-op.
    }

    CacheContinuousQueryEntry(K key, @Nullable V newVal, @Nullable GridCacheValueBytes newValBytes, @Nullable V oldVal,
        @Nullable GridCacheValueBytes oldValBytes) {

        this.key = key;
        this.newVal = newVal;
        this.newValBytes = newValBytes;
        this.oldVal = oldVal;
        this.oldValBytes = oldValBytes;
    }

    /**
     * @param cacheName Cache name.
     */
    void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return cache name.
     */
    String cacheName() {
        return cacheName;
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

    /**
     * @return Key.
     */
    K key() {
        return key;
    }

    /**
     * @return New value.
     */
    V value() {
        return newVal;
    }

    /**
     * @return Old value.
     */
    V oldValue() {
        return oldVal;
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
        return S.toString(CacheContinuousQueryEntry.class, this);
    }
}
