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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Cache queue proxy.
 */
public class GridCacheQueueProxy<T> implements IgniteQueue<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return F.t2();
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
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.add(item);
                    }
                }, cctx);

            return delegate.add(item);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(final T item) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.offer(item);
                    }
                }, cctx);

            return delegate.offer(item);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(final Collection<? extends T> items) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.addAll(items);
                    }
                }, cctx);

            return delegate.addAll(items);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SuspiciousMethodCalls")
    @Override public boolean contains(final Object item) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.contains(item);
                    }
                }, cctx);

            return delegate.contains(item);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(final Collection<?> items) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.containsAll(items);
                    }
                }, cctx);

            return delegate.containsAll(items);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        gate.enter();

        try {
            if (cctx.transactional()) {
                CU.outTx(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        delegate.clear();

                        return null;
                    }
                }, cctx);
            }
            else
                delegate.clear();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SuspiciousMethodCalls")
    @Override public boolean remove(final Object item) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.remove(item);
                    }
                }, cctx);

            return delegate.remove(item);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(final Collection<?> items) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.removeAll(items);
                    }
                }, cctx);

            return delegate.removeAll(items);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.isEmpty();
                    }
                }, cctx);

            return delegate.isEmpty();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Iterator<T>>() {
                    @Override public Iterator<T> call() throws Exception {
                        return delegate.iterator();
                    }
                }, cctx);

            return delegate.iterator();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Object[]>() {
                    @Override public Object[] call() throws Exception {
                        return delegate.toArray();
                    }
                }, cctx);

            return delegate.toArray();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
            if (cctx.transactional())
                return CU.outTx(new Callable<T1[]>() {
                    @Override public T1[] call() throws Exception {
                        return delegate.toArray(a);
                    }
                }, cctx);

            return delegate.toArray(a);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(final Collection<?> items) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.retainAll(items);
                    }
                }, cctx);

            return delegate.retainAll(items);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Integer>() {
                    @Override public Integer call() throws Exception {
                        return delegate.size();
                    }
                }, cctx);

            return delegate.size();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<T>() {
                    @Override public T call() throws Exception {
                        return delegate.poll();
                    }
                }, cctx);

            return delegate.poll();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T peek() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<T>() {
                    @Override public T call() throws Exception {
                        return delegate.peek();
                    }
                }, cctx);

            return delegate.peek();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(final int batchSize) {
        gate.enter();

        try {
            if (cctx.transactional()) {
                CU.outTx(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        delegate.clear(batchSize);

                        return null;
                    }
                }, cctx);
            }
            else
                delegate.clear(batchSize);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int remainingCapacity() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Integer>() {
                    @Override public Integer call() throws Exception {
                        return delegate.remainingCapacity();
                    }
                }, cctx);

            return delegate.remainingCapacity();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(final Collection<? super T> c) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Integer>() {
                    @Override public Integer call() throws Exception {
                        return delegate.drainTo(c);
                    }
                }, cctx);

            return delegate.drainTo(c);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public int drainTo(final Collection<? super T> c, final int maxElements) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Integer>() {
                    @Override public Integer call() throws Exception {
                        return delegate.drainTo(c, maxElements);
                    }
                }, cctx);

            return delegate.drainTo(c, maxElements);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public T remove() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<T>() {
                    @Override public T call() throws Exception {
                        return delegate.remove();
                    }
                }, cctx);

            return delegate.remove();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public T element() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<T>() {
                    @Override public T call() throws Exception {
                        return delegate.element();
                    }
                }, cctx);

            return delegate.element();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(final T item) {
        gate.enter();

        try {
            if (cctx.transactional()) {
                CU.outTx(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        delegate.put(item);

                        return null;
                    }
                }, cctx);
            }
            else
                delegate.put(item);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean offer(final T item, final long timeout, final TimeUnit unit) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        return delegate.offer(item, timeout, unit);
                    }
                }, cctx);

            return delegate.offer(item, timeout, unit);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T take() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<T>() {
                    @Override public T call() throws Exception {
                        return delegate.take();
                    }
                }, cctx);

            return delegate.take();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T poll(final long timeout, final TimeUnit unit) {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<T>() {
                    @Override public T call() throws Exception {
                        return delegate.poll(timeout, unit);
                    }
                }, cctx);

            return delegate.poll(timeout, unit);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        gate.enter();

        try {
            if (cctx.transactional()) {
                CU.outTx(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        delegate.close();

                        return null;
                    }
                }, cctx);
            }
            else
                delegate.close();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<GridKernalContext, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridKernalContext, String> t = stash.get();

            return t.get1().dataStructures().queue(t.get2(), 0, null);
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