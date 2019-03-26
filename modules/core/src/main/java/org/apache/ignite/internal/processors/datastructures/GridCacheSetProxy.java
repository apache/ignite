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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.NotNull;

/**
 * Cache set proxy.
 */
public class GridCacheSetProxy<T> extends GridCacheCollectionProxy<GridCacheSetImpl> implements IgniteSet<T>, Externalizable {
    /**
     * @param cctx Cache context.
     * @param delegate Delegate set.
     */
    public GridCacheSetProxy(GridCacheContext cctx, GridCacheSetImpl<T> delegate) {
        super(cctx, delegate);
    }

    /**
     * @return Set delegate.
     */
    public GridCacheSetImpl delegate() {
        return delegate;
    }

    @Override protected GridCacheSetImpl reStartDelegate() {
        try {
            GridCacheSetProxy proxy = (GridCacheSetProxy)cctx.dataStructures().set(name(), false, false, false);

            return proxy != null ? proxy.delegate() : null;
        }
        catch (IgniteCheckedException e) {
            return null;
        }
    }

    @Override protected void checkRemove() {
        boolean rmvd;

        try {
            rmvd = !delegate.checkHeader();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        rmvCheck = false;

        if (rmvd) {
            delegate.removed(true);

            //cctx.dataStructures().onRemoved(this);

            throw removedError();
        }
    }

    /**
     * Remove callback.
     */
    public void blockOnRemove() {
        delegate.removed(true);

        busyLock.block();
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
    @Override public boolean contains(final Object o) {
        enter();

        try {
            return delegate.contains(o);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        enter();

        try {
            return delegate.toArray();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T1> T1[] toArray(final T1[] a) {
        enter();

        try {
            return (T1[])delegate.toArray(a);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(final T t) {
        enter();

        try {
            return delegate.add(t);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final Object o) {
        enter();

        try {
            return delegate.remove(o);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(final Collection<?> c) {
        enter();

        try {
            return delegate.containsAll(c);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(final Collection<? extends T> c) {
        enter();

        try {
            return delegate.addAll(c);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(final Collection<?> c) {
        enter();

        try {
            return delegate.retainAll(c);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(final Collection<?> c) {
        enter();

        try {
            return delegate.removeAll(c);
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
    @Override public void close() {
        IgniteFuture<Boolean> destroyFut = null;

        gate.enter();

        try {
            delegate.close();

            if (delegate.separated()) {
                IgniteInternalFuture<Boolean> fut = cctx.kernalContext().cache().dynamicDestroyCache(
                    cctx.cache().name(), false, true, false, null);

                ((GridFutureAdapter)fut).ignoreInterrupts();

                destroyFut = new IgniteFutureImpl<>(fut);
            }
        }
        finally {
            gate.leave();
        }

        if (destroyFut != null)
            destroyFut.get();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
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
    @Override public String toString() {
        return delegate.toString();
    }
}
