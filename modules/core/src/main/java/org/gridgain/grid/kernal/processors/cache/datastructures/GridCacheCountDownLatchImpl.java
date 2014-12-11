/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Cache count down latch implementation.
 */
public final class GridCacheCountDownLatchImpl implements GridCacheCountDownLatchEx, Externalizable {
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

    /** Latch name. */
    private String name;

    /** Removed flag.*/
    private volatile boolean rmvd;

    /** Latch key. */
    private GridCacheInternalKey key;

    /** Latch projection. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheCountDownLatchValue> latchView;

    /** Cache context. */
    private GridCacheContext ctx;

    /** Current count. */
    private int cnt;

    /** Initial count. */
    private int initCnt;

    /** Auto delete flag. */
    private boolean autoDel;

    /** Internal latch (transient). */
    private volatile CountDownLatch internalLatch;

    /** Initialization guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Initialization latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheCountDownLatchImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param name Latch name.
     * @param cnt Current count.
     * @param initCnt Initial count.
     * @param autoDel Auto delete flag.
     * @param key Latch key.
     * @param latchView Latch projection.
     * @param ctx Cache context.
     */
    public GridCacheCountDownLatchImpl(String name, int cnt, int initCnt, boolean autoDel, GridCacheInternalKey key,
        GridCacheProjection<GridCacheInternalKey, GridCacheCountDownLatchValue> latchView, GridCacheContext ctx) {
        assert name != null;
        assert cnt >= 0;
        assert initCnt >= 0;
        assert key != null;
        assert latchView != null;
        assert ctx != null;

        this.name = name;
        this.cnt = cnt;
        this.initCnt = initCnt;
        this.autoDel = autoDel;
        this.key = key;
        this.latchView = latchView;
        this.ctx = ctx;

        log = ctx.gridConfig().getGridLogger().getLogger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public int initialCount() {
        return initCnt;
    }

    /** {@inheritDoc} */
    @Override public boolean autoDelete() {
        return autoDel;
    }

    /** {@inheritDoc} */
    @Override public void await() throws IgniteCheckedException {
        initializeLatch();

        U.await(internalLatch);
    }

    /** {@inheritDoc} */
    @Override public boolean await(long timeout, TimeUnit unit) throws IgniteCheckedException {
        initializeLatch();

        return U.await(internalLatch, timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public boolean await(long timeout) throws IgniteCheckedException {
        return await(timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public int countDown() throws IgniteCheckedException {
        return CU.outTx(new CountDownCallable(1), ctx);
    }

    /** {@inheritDoc} */
    @Override public int countDown(int val) throws IgniteCheckedException {
        A.ensure(val > 0, "val should be positive");

        return CU.outTx(new CountDownCallable(val), ctx);
    }

    /** {@inheritDoc}*/
    @Override public void countDownAll() throws IgniteCheckedException {
        CU.outTx(new CountDownCallable(0), ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        assert cnt == 0;

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

    /** {@inheritDoc} */
    @Override public void onUpdate(int cnt) {
        assert cnt >= 0;

        this.cnt = cnt;

        while (internalLatch != null && internalLatch.getCount() > cnt)
            internalLatch.countDown();
    }

    /**
     * @throws IgniteCheckedException If operation failed.
     */
    private void initializeLatch() throws IgniteCheckedException {
        if (initGuard.compareAndSet(false, true)) {
            try {
                internalLatch = CU.outTx(
                    new Callable<CountDownLatch>() {
                        @Override public CountDownLatch call() throws Exception {
                            GridCacheTx tx = CU.txStartInternal(ctx, latchView, PESSIMISTIC, REPEATABLE_READ);

                            try {
                                GridCacheCountDownLatchValue val = latchView.get(key);

                                if (val == null) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to find count down latch with given name: " + name);

                                    assert cnt == 0;

                                    return new CountDownLatch(cnt);
                                }

                                tx.commit();

                                return new CountDownLatch(val.get());
                            }
                            finally {
                                tx.close();
                            }
                        }
                    },
                    ctx
                );

                if (log.isDebugEnabled())
                    log.debug("Initialized internal latch: " + internalLatch);
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            U.await(initLatch);

            if (internalLatch == null)
                throw new IgniteCheckedException("Internal latch has not been properly initialized.");
        }
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
    @SuppressWarnings({"ConstantConditions"})
    private Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridCacheContext, String> t = stash.get();

            return t.get1().dataStructures().countDownLatch(t.get2(), 0, false, false);
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
        return S.toString(GridCacheCountDownLatchImpl.class, this);
    }

    /**
     *
     */
    private class CountDownCallable implements Callable<Integer> {
        /** Value to count down on (if 0 then latch is counted down to 0). */
        private final int val;

        /**
         * @param val Value to count down on (if 0 is passed latch is counted down to 0).
         */
        private CountDownCallable(int val) {
            assert val >= 0;

            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            GridCacheTx tx = CU.txStartInternal(ctx, latchView, PESSIMISTIC, REPEATABLE_READ);

            try {
                GridCacheCountDownLatchValue latchVal = latchView.get(key);

                if (latchVal == null) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to find count down latch with given name: " + name);

                    assert cnt == 0;

                    return cnt;
                }

                int retVal;

                if (val > 0) {
                    retVal = latchVal.get() - val;

                    if (retVal < 0)
                        retVal = 0;
                }
                else
                    retVal = 0;

                latchVal.set(retVal);

                latchView.put(key, latchVal);

                tx.commit();

                return retVal;
            }
            finally {
                tx.close();
            }
        }
    }
}
