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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Cache set proxy.
 */
public class GridCacheSetProxy<T> implements IgniteSet<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Delegate set. */
    private GridCacheSetImpl<T> delegate;

    /** Cache context. */
    private GridCacheContext cctx;

    /** Cache gateway. */
    private GridCacheGateway gate;

    /** Busy lock. */
    private GridSpinBusyLock busyLock;

    /** Check removed flag. */
    private boolean rmvCheck;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetProxy() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param delegate Delegate set.
     */
    public GridCacheSetProxy(GridCacheContext cctx, GridCacheSetImpl<T> delegate) {
        this.cctx = cctx;
        this.delegate = delegate;

        gate = cctx.gate();

        busyLock = new GridSpinBusyLock();
    }

    /**
     * @return Set delegate.
     */
    public GridCacheSetImpl delegate() {
        return delegate;
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
        enterBusy();

        try {
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
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        enterBusy();

        try {
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
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(final Object o) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.contains(o);
                        }
                    }, cctx);

                return delegate.contains(o);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        enterBusy();

        try {
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
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T1> T1[] toArray(final T1[] a) {
        enterBusy();

        try {
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
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(final T t) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.add(t);
                        }
                    }, cctx);

                return delegate.add(t);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final Object o) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.remove(o);
                        }
                    }, cctx);

                return delegate.remove(o);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(final Collection<?> c) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.containsAll(c);
                        }
                    }, cctx);

                return delegate.containsAll(c);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(final Collection<? extends T> c) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.addAll(c);
                        }
                    }, cctx);

                return delegate.addAll(c);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(final Collection<?> c) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.retainAll(c);
                        }
                    }, cctx);

                return delegate.retainAll(c);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(final Collection<?> c) {
        enterBusy();

        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.removeAll(c);
                        }
                    }, cctx);

                return delegate.removeAll(c);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        enterBusy();

        try {
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
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        enterBusy();

        try {
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
        finally {
            leaveBusy();
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
    @Override public boolean collocated() {
        return delegate.collocated();
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return delegate.removed();
    }

    /**
     * Enters busy state.
     */
    private void enterBusy() {
        boolean rmvd;

        if (rmvCheck) {
            try {
                rmvd = !delegate().checkHeader();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            rmvCheck = false;

            if (rmvd) {
                delegate.removed(true);

                cctx.dataStructures().onRemoved(this);

                throw removedError();
            }
        }

        if (!busyLock.enterBusy())
            throw removedError();
    }

    /**
     *
     */
    public void needCheckNotRemoved() {
        rmvCheck = true;
    }

    /**
     * @return Error.
     */
    private IllegalStateException removedError() {
        return new IllegalStateException("Set has been removed from cache: " + delegate);
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
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

            return t.get1().dataStructures().set(t.get2(), null);
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