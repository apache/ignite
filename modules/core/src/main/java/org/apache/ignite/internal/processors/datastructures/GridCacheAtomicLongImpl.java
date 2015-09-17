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
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.util.typedef.internal.CU.retryTopologySafe;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache atomic long implementation.
 */
public final class GridCacheAtomicLongImpl implements GridCacheAtomicLongEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Atomic long name. */
    private String name;

    /** Removed flag.*/
    private volatile boolean rmvd;

    /** Check removed flag. */
    private boolean rmvCheck;

    /** Atomic long key. */
    private GridCacheInternalKey key;

    /** Atomic long projection. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicLongValue> atomicView;

    /** Cache context. */
    private GridCacheContext ctx;

    /** Callable for {@link #get()}. */
    private final Callable<Long> getCall = new Callable<Long>() {
        @Override public Long call() throws Exception {
            GridCacheAtomicLongValue val = atomicView.get(key);

            if (val == null)
                throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

            return val.get();
        }
    };

    /** Callable for {@link #incrementAndGet()}. */
    private final Callable<Long> incAndGetCall = retryTopologySafe(new Callable<Long>() {
        @Override public Long call() throws Exception {
            try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                GridCacheAtomicLongValue val = atomicView.get(key);

                if (val == null)
                    throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                long retVal = val.get() + 1;

                val.set(retVal);

                atomicView.put(key, val);

                tx.commit();

                return retVal;
            }
            catch (Error | Exception e) {
                U.error(log, "Failed to increment and get: " + this, e);

                throw e;
            }
        }
    });

    /** Callable for {@link #getAndIncrement()}. */
    private final Callable<Long> getAndIncCall = retryTopologySafe(new Callable<Long>() {
        @Override public Long call() throws Exception {
            try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                GridCacheAtomicLongValue val = atomicView.get(key);

                if (val == null)
                    throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                long retVal = val.get();

                val.set(retVal + 1);

                atomicView.put(key, val);

                tx.commit();

                return retVal;
            }
            catch (Error | Exception e) {
                U.error(log, "Failed to get and increment: " + this, e);

                throw e;
            }
        }
    });

    /** Callable for {@link #decrementAndGet()}. */
    private final Callable<Long> decAndGetCall = retryTopologySafe(new Callable<Long>() {
        @Override public Long call() throws Exception {
            try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                GridCacheAtomicLongValue val = atomicView.get(key);

                if (val == null)
                    throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                long retVal = val.get() - 1;

                val.set(retVal);

                atomicView.put(key, val);

                tx.commit();

                return retVal;
            }
            catch (Error | Exception e) {
                U.error(log, "Failed to decrement and get: " + this, e);

                throw e;
            }
        }
    });

    /** Callable for {@link #getAndDecrement()}. */
    private final Callable<Long> getAndDecCall = retryTopologySafe(new Callable<Long>() {
        @Override public Long call() throws Exception {
            try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                GridCacheAtomicLongValue val = atomicView.get(key);

                if (val == null)
                    throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                long retVal = val.get();

                val.set(retVal - 1);

                atomicView.put(key, val);

                tx.commit();

                return retVal;
            }
            catch (Error | Exception e) {
                U.error(log, "Failed to get and decrement and get: " + this, e);

                throw e;
            }
        }
    });

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheAtomicLongImpl() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Atomic long name.
     * @param key Atomic long key.
     * @param atomicView Atomic projection.
     * @param ctx CacheContext.
     */
    public GridCacheAtomicLongImpl(String name, GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicLongValue> atomicView, GridCacheContext ctx) {
        assert key != null;
        assert atomicView != null;
        assert ctx != null;
        assert name != null;

        this.ctx = ctx;
        this.key = key;
        this.atomicView = atomicView;
        this.name = name;

        log = ctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        checkRemoved();

        try {
            return CU.outTx(getCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() {
        checkRemoved();

        try{
            return CU.outTx(incAndGetCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() {
        checkRemoved();

        try {
            return CU.outTx(getAndIncCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) {
        checkRemoved();

        try {
            return CU.outTx(internalAddAndGet(l), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) {
        checkRemoved();

        try {
            return CU.outTx(internalGetAndAdd(l), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long decrementAndGet() {
        checkRemoved();

        try {
            return CU.outTx(decAndGetCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndDecrement() {
        checkRemoved();

        try {
            return CU.outTx(getAndDecCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndSet(long l) {
        checkRemoved();

        try {
            return CU.outTx(internalGetAndSet(l), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(long expVal, long newVal) {
        checkRemoved();

        try {
            return CU.outTx(internalCompareAndSet(expVal, newVal), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Check removed flag.
     *
     * @throws IllegalStateException If removed.
     */
    private void checkRemoved() throws IllegalStateException {
        if (rmvd)
            throw removedError();

        if (rmvCheck) {
            try {
                rmvd = atomicView.get(key) == null;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            rmvCheck = false;

            if (rmvd) {
                ctx.kernalContext().dataStructures().onRemoved(key, this);

                throw removedError();
            }
        }
    }

    /**
     * @return Error.
     */
    private IllegalStateException removedError() {
        return new IllegalStateException("Atomic long was removed from cache: " + name);
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return rmvd = true;
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        rmvCheck = true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheInternalKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (rmvd)
            return;

        try {
            ctx.kernalContext().dataStructures().removeAtomicLong(name);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Method returns callable for execution {@link #addAndGet(long)} operation in async and sync mode.
     *
     * @param l Value will be added to atomic long.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Long> internalAddAndGet(final long l) {
        return retryTopologySafe(new Callable<Long>() {
            @Override public Long call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicLongValue val = atomicView.get(key);

                    if (val == null)
                        throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                    long retVal = val.get() + l;

                    val.set(retVal);

                    atomicView.put(key, val);

                    tx.commit();

                    return retVal;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to add and get: " + this, e);

                    throw e;
                }
            }
        });
    }

    /**
     * Method returns callable for execution {@link #getAndAdd(long)} operation in async and sync mode.
     *
     * @param l Value will be added to atomic long.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Long> internalGetAndAdd(final long l) {
        return retryTopologySafe(new Callable<Long>() {
            @Override public Long call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicLongValue val = atomicView.get(key);

                    if (val == null)
                        throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                    long retVal = val.get();

                    val.set(retVal + l);

                    atomicView.put(key, val);

                    tx.commit();

                    return retVal;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to get and add: " + this, e);

                    throw e;
                }
            }
        });
    }

    /**
     * Method returns callable for execution {@link #getAndSet(long)} operation in async and sync mode.
     *
     * @param l Value will be added to atomic long.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Long> internalGetAndSet(final long l) {
        return new Callable<Long>() {
            @Override public Long call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicLongValue val = atomicView.get(key);

                    if (val == null)
                        throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                    long retVal = val.get();

                    val.set(l);

                    atomicView.put(key, val);

                    tx.commit();

                    return retVal;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to get and set: " + this, e);

                    throw e;
                }
            }
        };
    }

    /**
     * Method returns callable for execution {@link #compareAndSet(long, long)}
     * operation in async and sync mode.
     *
     * @param expVal Expected atomic long value.
     * @param newVal New atomic long value.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalCompareAndSet(final long expVal, final long newVal) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicLongValue val = atomicView.get(key);

                    if (val == null)
                        throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                    boolean retVal = val.get() == expVal;

                    if (retVal) {
                        val.set(newVal);

                        atomicView.getAndPut(key, val);

                        tx.commit();
                    }

                    return retVal;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to compare and set: " + this, e);

                    throw e;
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx.kernalContext());
        out.writeUTF(name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<GridKernalContext, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(in.readUTF());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridKernalContext, String> t = stash.get();

            return t.get1().dataStructures().atomicLong(t.get2(), 0L, false);
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
        return S.toString(GridCacheAtomicLongImpl.class, this);
    }
}
