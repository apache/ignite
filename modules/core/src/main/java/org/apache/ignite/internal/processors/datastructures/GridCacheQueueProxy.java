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
public class GridCacheQueueProxy<T> extends GridCacheCollectionProxy<GridCacheQueueAdapter> implements IgniteQueue<T> {
    /** */
    private static final long serialVersionUID = 0L;

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
        super(cctx, delegate);
    }

    /**
     * @return Delegate queue.
     */
    public GridCacheQueueAdapter<T> delegate() {
        return delegate;
    }

    @Override protected GridCacheQueueAdapter reStartDelegate() {
        try {
            GridCacheQueueProxy<Object> proxy = cctx.dataStructures().queue0(name(), 0, false, false);

            return proxy != null ? proxy.delegate() : null;
        }
        catch (IgniteCheckedException e) {
            return null;
        }
    }

    @Override protected void checkRemove() {

    }

    /** {@inheritDoc} */
    @Override public boolean add(final T item) {
        enter();

        try {
            return delegate.add(item);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(final T item) {
        enter();

        try {
            return delegate.offer(item);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(final Collection<? extends T> items) {
        enter();

        try {
            return delegate.addAll(items);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(final Object item) {
        enter();

        try {
            return delegate.contains(item);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(final Collection<?> items) {
        enter();

        try {
            return delegate.containsAll(items);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        enter();

        try {
            delegate.clear();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final Object item) {
        enter();

        try {
            return delegate.remove(item);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(final Collection<?> items) {
        enter();

        try {
            return delegate.removeAll(items);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        enter();

        try {
            return delegate.isEmpty();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        enter();

        try {
            return delegate.iterator();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        enter();

        try {
            return delegate.toArray();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SuspiciousToArrayCall")
    @Override public <T1> T1[] toArray(final T1[] a) {
        enter();

        try {
            return (T1[])delegate.toArray(a);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(final Collection<?> items) {
        enter();

        try {
            return delegate.retainAll(items);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        enter();

        try {
            return delegate.size();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll() {
        enter();

        try {
            return (T)delegate.poll();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T peek() {
        enter();

        try {
            return (T)delegate.peek();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(final int batchSize) {
        enter();

        try {
            delegate.clear(batchSize);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int remainingCapacity() {
        enter();

        try {
            return delegate.remainingCapacity();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(final Collection<? super T> c) {
        enter();

        try {
            return delegate.drainTo(c);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(final Collection<? super T> c, final int maxElements) {
        enter();

        try {
            return delegate.drainTo(c, maxElements);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public T remove() {
        enter();

        try {
            return (T)delegate.remove();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public T element() {
        enter();

        try {
            return (T)delegate.element();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(final T item) {
        enter();

        try {
            delegate.put(item);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(final T item, final long timeout, final TimeUnit unit) {
        enter();

        try {
            return delegate.offer(item, timeout, unit);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() {
        enter();

        try {
            return (T)delegate.take();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll(final long timeout, final TimeUnit unit) {
        enter();

        try {
            return (T)delegate.poll(timeout, unit);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        enter();

        try {
            delegate.close();
        }
        finally {
            leave();
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
        return (R)delegate.affinityCall(job);
    }

    /** {@inheritDoc} */
    @Override public <V1> IgniteQueue<V1> withKeepBinary() {
        enter();

        try {
            return new GridCacheQueueProxy<V1>(cctx, (GridCacheQueueAdapter<V1>)delegate.withKeepBinary());
        }
        finally {
            leave();
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
}
