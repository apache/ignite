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

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class ClusterNodeLocalMapImpl<K, V> extends ConcurrentHashMap8<K, V> implements ClusterNodeLocalMap<K, V>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ThreadLocal<String> stash = new ThreadLocal<>();

    /** */
    private GridMetadataAwareAdapter impl = new GridMetadataAwareAdapter();

    /** */
    private GridKernalContext ctx;

    /**
     * No-arg constructor is required by externalization.
     */
    public ClusterNodeLocalMapImpl() {
        // No-op.
    }

    /**
     *
     * @param ctx Kernal context.
     */
    ClusterNodeLocalMapImpl(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V addIfAbsent(K key, @Nullable Callable<V> dflt) {
        return F.addIfAbsent(this, key, dflt);
    }

    /** {@inheritDoc} */
    @Override public V addIfAbsent(K key, V val) {
        return F.addIfAbsent(this, key, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMeta(String name, V val) {
        return impl.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V putMetaIfAbsent(String name, V val) {
        return impl.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V putMetaIfAbsent(String name, Callable<V> c) {
        return impl.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, V val) {
        return impl.addMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, Callable<V> c) {
        return impl.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable
    @Override public <V> V meta(String name) {
        return (V)impl.meta(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable
    @Override public <V> V removeMeta(String name) {
        return (V)impl.removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean removeMeta(String name, V val) {
        return impl.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> Map<String, V> allMeta() {
        return impl.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return impl.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean hasMeta(String name, V val) {
        return impl.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean replaceMeta(String name, V curVal, V newVal) {
        return impl.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        impl.copyMeta(from);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        impl.copyMeta(data);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ctx.gridName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stash.set(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            return GridGainEx.gridx(stash.get()).nodeLocalMap();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterNodeLocalMapImpl.class, this);
    }
}

