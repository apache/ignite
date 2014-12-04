/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Cache atomic stamped implementation.
 */
public final class GridCacheAtomicStampedImpl<T, S> implements GridCacheAtomicStampedEx<T, S>, Externalizable {
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
    private GridLogger log;

    /** Atomic stamped name. */
    private String name;

    /** Removed flag.*/
    private volatile boolean rmvd;

    /** Atomic stamped key. */
    private GridCacheInternalKey key;

    /** Atomic stamped projection. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>> atomicView;

    /** Cache context. */
    private GridCacheContext ctx;

    /** Callable for {@link #get()} operation */
    private final Callable<IgniteBiTuple<T, S>> getCall = new Callable<IgniteBiTuple<T, S>>() {
        @Override public IgniteBiTuple<T, S> call() throws Exception {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new GridException("Failed to find atomic stamped with given name: " + name);

            return stmp.get();
        }
    };

    /** Callable for {@link #value()} operation */
    private final Callable<T> valCall = new Callable<T>() {
        @Override public T call() throws Exception {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new GridException("Failed to find atomic stamped with given name: " + name);

            return stmp.value();
        }
    };

    /** Callable for {@link #stamp()} operation */
    private final Callable<S> stampCall = new Callable<S>() {
        @Override public S call() throws Exception {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new GridException("Failed to find atomic stamped with given name: " + name);

            return stmp.stamp();
        }
    };

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheAtomicStampedImpl() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Atomic stamped name.
     * @param key Atomic stamped key.
     * @param atomicView Atomic projection.
     * @param ctx Cache context.
     */
    public GridCacheAtomicStampedImpl(String name, GridCacheInternalKey key, GridCacheProjection<GridCacheInternalKey,
        GridCacheAtomicStampedValue<T, S>> atomicView, GridCacheContext ctx) {
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
    @Override public IgniteBiTuple<T, S> get() throws GridException {
        checkRemoved();

        return CU.outTx(getCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public void set(T val, S stamp) throws GridException {
        checkRemoved();

        CU.outTx(internalSet(val, stamp), ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(T expVal, T newVal, S expStamp, S newStamp) throws GridException {
        checkRemoved();

        return CU.outTx(internalCompareAndSet(F0.equalTo(expVal), wrapperClosure(newVal),
            F0.equalTo(expStamp), wrapperClosure(newStamp)), ctx);
    }

    /** {@inheritDoc} */
    @Override public S stamp() throws GridException {
        checkRemoved();

        return CU.outTx(stampCall, ctx);
    }

    /** {@inheritDoc} */
    @Override public T value() throws GridException {
        checkRemoved();

        return CU.outTx(valCall, ctx);
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
     * Method make wrapper closure for existing value.
     *
     * @param val Value.
     * @return Closure.
     */
    private <N> GridClosure<N, N> wrapperClosure(final N val) {
        return new GridClosure<N, N>() {
            @Override public N apply(N e) {
                return val;
            }
        };
    }

    /**
     * Method returns callable for execution {@link #set(Object,Object)}} operation in async and sync mode.
     *
     * @param val Value will be set in the atomic stamped.
     * @param stamp Stamp will be set in the atomic stamped.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalSet(final T val, final S stamp) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {

                GridCacheTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

                try {
                    GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

                    if (stmp == null)
                        throw new GridException("Failed to find atomic stamped with given name: " + name);

                    stmp.set(val, stamp);

                    atomicView.put(key, stmp);

                    tx.commit();

                    return true;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to set [val=" + val + ", stamp=" + stamp + ", atomicStamped=" + this + ']', e);

                    throw e;
                } finally {
                    tx.close();
                }
            }
        };
    }

    /**
     * Conditionally asynchronously sets the new value and new stamp. They will be set if
     * {@code expValPred} and {@code expStampPred} both evaluate to {@code true}.
     *
     * @param expValPred Predicate which should evaluate to {@code true} for value to be set
     * @param newValClos Closure generates new value.
     * @param expStampPred Predicate which should evaluate to {@code true} for value to be set
     * @param newStampClos Closure generates new stamp value.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalCompareAndSet(final GridPredicate<T> expValPred,
        final GridClosure<T, T> newValClos, final GridPredicate<S> expStampPred,
        final GridClosure<S, S> newStampClos) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                GridCacheTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ);

                try {
                    GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

                    if (stmp == null)
                        throw new GridException("Failed to find atomic stamped with given name: " + name);

                    if (!(expValPred.apply(stmp.value()) && expStampPred.apply(stmp.stamp()))) {
                        tx.setRollbackOnly();

                        return false;
                    }
                    else {
                        stmp.set(newValClos.apply(stmp.value()), newStampClos.apply(stmp.stamp()));

                        atomicView.put(key, stmp);

                        tx.commit();

                        return true;
                    }
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to compare and set [expValPred=" + expValPred + ", newValClos=" +
                        newValClos + ", expStampPred=" + expStampPred + ", newStampClos=" + newStampClos +
                        ", atomicStamped=" + this + ']', e);

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
    @SuppressWarnings("unchecked")
    private Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridCacheContext, String> t = stash.get();

            return t.get1().dataStructures().atomicStamped(t.get2(), null, null, false);
        }
        catch (GridException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /**
     * Check removed status.
     *
     * @throws GridException If removed.
     */
    private void checkRemoved() throws GridException {
        if (rmvd)
            throw new GridCacheDataStructureRemovedException("Atomic stamped was removed from cache: " + name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridCacheAtomicStampedImpl.class, this);
    }
}
