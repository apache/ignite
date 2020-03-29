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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/**
 * Cache queue proxy.
 */
public class GridCacheQueueProxy<T> implements IgniteQueue<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<T3<GridKernalContext, String, String>> stash =
        new ThreadLocal<T3<GridKernalContext, String, String>>() {
            @Override protected T3<GridKernalContext, String, String> initialValue() {
                return new T3<>();
            }
        };

    /** Delegate queue. */
    private GridCacheQueueAdapter<T> delegate;

    /** Cache context. */
    private GridCacheContext cctx;

    /** Cache gateway. */
    private GridCacheGateway gate;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueueProxy() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param delegate Delegate queue.
     */
    public GridCacheQueueProxy(GridCacheContext cctx, GridCacheQueueAdapter<T> delegate) {
        this.cctx = cctx;
        this.delegate = delegate;

        gate = cctx.gate();
    }

    /**
     * @return Delegate queue.
     */
    public GridCacheQueueAdapter<T> delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public boolean add(final T item) {
        gate.enter();

        try {
            return delegate.add(item);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(final T item) {
        gate.enter();

        try {
            return delegate.offer(item);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(final Collection<? extends T> items) {
        gate.enter();

        try {
            return delegate.addAll(items);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(final Object item) {
        gate.enter();

        try {
            return delegate.contains(item);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(final Collection<?> items) {
        gate.enter();

        try {
            return delegate.containsAll(items);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        gate.enter();

        try {
            delegate.clear();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final Object item) {
        gate.enter();

        try {
            return delegate.remove(item);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(final Collection<?> items) {
        gate.enter();

        try {
            return delegate.removeAll(items);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        gate.enter();

        try {
            return delegate.isEmpty();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        gate.enter();

        try {
            return delegate.iterator();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        gate.enter();

        try {
            return delegate.toArray();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SuspiciousToArrayCall")
    @Override public <T1> T1[] toArray(final T1[] a) {
        gate.enter();

        try {
            return delegate.toArray(a);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(final Collection<?> items) {
        gate.enter();

        try {
            return delegate.retainAll(items);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        gate.enter();

        try {
            return delegate.size();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll() {
        gate.enter();

        try {
            return delegate.poll();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T peek() {
        gate.enter();

        try {
            return delegate.peek();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(final int batchSize) {
        gate.enter();

        try {
            delegate.clear(batchSize);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int remainingCapacity() {
        gate.enter();

        try {
            return delegate.remainingCapacity();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(final Collection<? super T> c) {
        gate.enter();

        try {
            return delegate.drainTo(c);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(final Collection<? super T> c, final int maxElements) {
        gate.enter();

        try {
            return delegate.drainTo(c, maxElements);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public T remove() {
        gate.enter();

        try {
            return delegate.remove();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public T element() {
        gate.enter();

        try {
            return delegate.element();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(final T item) {
        gate.enter();

        try {
            delegate.put(item);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(final T item, final long timeout, final TimeUnit unit) {
        gate.enter();

        try {
            return delegate.offer(item, timeout, unit);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() {
        gate.enter();

        try {
            return delegate.take();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll(final long timeout, final TimeUnit unit) {
        gate.enter();

        try {
            return delegate.poll(timeout, unit);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        gate.enter();

        try {
            delegate.close();
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return delegate.capacity();
    }

    /** {@inheritDoc} */
    @Override public boolean bounded() {
        return delegate.bounded();
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        return delegate.collocated();
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return delegate.removed();
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(final IgniteRunnable job) {
        delegate.affinityRun(job);
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(final IgniteCallable<R> job) {
        return delegate.affinityCall(job);
    }

    /** {@inheritDoc} */
    @Override public <V1> IgniteQueue<V1> withKeepBinary() {
        gate.enter();

        try {
            return new GridCacheQueueProxy<>(cctx, (GridCacheQueueAdapter<V1>)delegate.withKeepBinary());
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return delegate.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueueProxy that = (GridCacheQueueProxy)o;

        return delegate.equals(that.delegate);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx.kernalContext());
        U.writeString(out, name());
        U.writeString(out, cctx.group().name());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        T3<GridKernalContext, String, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(U.readString(in));
        t.set3(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            T3<GridKernalContext, String, String> t = stash.get();

            return t.get1().dataStructures().queue(t.get2(), t.get3(), 0, null);
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}
