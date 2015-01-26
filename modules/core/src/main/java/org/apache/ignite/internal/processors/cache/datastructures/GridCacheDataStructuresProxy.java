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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.internal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Data structures proxy object.
 */
public class GridCacheDataStructuresProxy<K, V> implements CacheDataStructures, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Delegate object. */
    private CacheDataStructures delegate;

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheDataStructuresProxy() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param delegate Delegate object.
     */
    public GridCacheDataStructuresProxy(GridCacheContext<K, V> cctx, CacheDataStructures delegate) {
        this.delegate = delegate;
        this.cctx = cctx;

        gate = cctx.gate();
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicSequence(name, initVal, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicSequence(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicSequence(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicLong(name, initVal, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicLong(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicLong(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, T initVal, boolean create)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicReference(name, initVal, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicReference(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicReference(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, T initVal, S initStamp,
        boolean create) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicStamped(name, initVal, initStamp, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicStamped(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicStamped(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap, boolean collocated, boolean create)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.queue(name, cap, collocated, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeQueue(name, 0);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name, int batchSize) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeQueue(name, batchSize);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteSet<T> set(String name, boolean collocated, boolean create)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.set(name, collocated, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeSet(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeSet(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.countDownLatch(name, cnt, autoDel, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeCountDownLatch(String name) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeCountDownLatch(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cctx = (GridCacheContext<K, V>)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return cctx.grid().cache(cctx.cache().name()).dataStructures();
    }
}
