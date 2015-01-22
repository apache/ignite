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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Cache atomic long implementation.
 */
public final class GridCacheAtomicLongImpl implements GridCacheAtomicLongEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridCacheContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridCacheContext, String>>() {
            @Override protected IgniteBiTuple<GridCacheContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Atomic long name. */
    private String name;

    /** Removed flag.*/
    private volatile boolean rmvd;

    /** Atomic long key. */
    private GridCacheInternalKey key;

    /** Atomic long projection. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicLongValue> atomicView;

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
    private final Callable<Long> incAndGetCall = new Callable<Long>() {
        @Override public Long call() throws Exception {
            IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

            try {
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
            } finally {
                tx.close();
            }
        }
    };

    /** Callable for {@link #getAndIncrement()}. */
    private final Callable<Long> getAndIncCall = new Callable<Long>() {
        @Override public Long call() throws Exception {
            IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

            try {
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
            } finally {
                tx.close();
            }
        }
    };

    /** Callable for {@link #decrementAndGet()}. */
    private final Callable<Long> decAndGetCall = new Callable<Long>() {
        @Override public Long call() throws Exception {
            IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

            try {
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
            } finally {
                tx.close();
            }
        }
    };

    /** Callable for {@link #getAndDecrement()}. */
    private final Callable<Long> getAndDecCall = new Callable<Long>() {
        @Override public Long call() throws Exception {
            IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

            try {
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
            } finally {
                tx.close();
            }
        }
    };

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
        GridCacheProjection<GridCacheInternalKey, GridCacheAtomicLongValue> atomicView, GridCacheContext ctx) {
        assert key != null;
        assert atomicView != null;
        assert ctx != null;
        assert name != null;

        this.ctx = ctx;
        this.key = key;
        this.atomicView = atomicView;
        this.name = name;

        log = ctx.gridConfig().getGridLogger().getLogger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long get() throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(getCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(incAndGetCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(getAndIncCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(internalAddAndGet(l), ctx);
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(internalGetAndAdd(l), ctx);
    }

    /** {@inheritDoc} */
    @Override public long decrementAndGet() throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(decAndGetCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public long getAndDecrement() throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(getAndDecCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public long getAndSet(long l) throws IgniteCheckedException {
        checkRemoved();

        return CU.outTx(internalGetAndSet(l), ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(long expVal, long newVal)
        throws IgniteCheckedException {
        checkRemoved();
        return CU.outTx(internalCompareAndSet(expVal, newVal), ctx);
    }

    /**
     * Check removed flag.
     *
     * @throws IgniteCheckedException If removed.
     */
    private void checkRemoved() throws IgniteCheckedException {
        if (rmvd)
            throw new GridCacheDataStructureRemovedException("Atomic long was removed from cache: " + name);
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return rmvd = true;
    }

    /** {@inheritDoc} */
    @Override public void onInvalid(@Nullable Exception err) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCacheInternalKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /**
     * Method returns callable for execution {@link #addAndGet(long)} operation in async and sync mode.
     *
     * @param l Value will be added to atomic long.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Long> internalAddAndGet(final long l) {
        return new Callable<Long>() {
            @Override public Long call() throws Exception {
                IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

                try {
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
                } finally {
                    tx.close();
                }
            }
        };
    }

    /**
     * Method returns callable for execution {@link #getAndAdd(long)} operation in async and sync mode.
     *
     * @param l Value will be added to atomic long.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Long> internalGetAndAdd(final long l) {
        return new Callable<Long>() {
            @Override public Long call() throws Exception {
                IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

                try {
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
                } finally {
                    tx.close();
                }
            }
        };
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
                IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

                try {
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
                } finally {
                    tx.close();
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
                IgniteTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

                try {
                    GridCacheAtomicLongValue val = atomicView.get(key);

                    if (val == null)
                        throw new IgniteCheckedException("Failed to find atomic long with given name: " + name);

                    boolean retVal = val.get() == expVal;

                    if (retVal) {
                        val.set(newVal);

                        atomicView.put(key, val);

                        tx.commit();
                    }

                    return retVal;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to compare and set: " + this, e);

                    throw e;
                } finally {
                    tx.close();
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
        out.writeUTF(name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<GridCacheContext, String> t = stash.get();

        t.set1((GridCacheContext)in.readObject());
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
            IgniteBiTuple<GridCacheContext, String> t = stash.get();

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
