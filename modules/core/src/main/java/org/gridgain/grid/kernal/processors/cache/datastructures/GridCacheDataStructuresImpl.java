/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

/**
 * Data structures implementation object.
 */
public class GridCacheDataStructuresImpl<K, V> implements GridCacheDataStructures {
    /** Data structures manager. */
    private GridCacheDataStructuresManager<K, V> dsMgr;

    /**
     * @param cctx Cache context.
     */
    public GridCacheDataStructuresImpl(GridCacheContext<K, V> cctx) {
        dsMgr = cctx.dataStructures();
    }

    /** {@inheritDoc} */
    @Override public GridCacheAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws GridException {
        return dsMgr.sequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicSequence(String name) throws GridException {
        return dsMgr.removeSequence(name);
    }

    /** {@inheritDoc} */
    @Override public GridCacheAtomicLong atomicLong(String name, long initVal, boolean create) throws GridException {
        return dsMgr.atomicLong(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicLong(String name) throws GridException {
        return dsMgr.removeAtomicLong(name);
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheAtomicReference<T> atomicReference(String name, T initVal, boolean create)
        throws GridException {
        return dsMgr.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicReference(String name) throws GridException {
        return dsMgr.removeAtomicReference(name);
    }

    /** {@inheritDoc} */
    @Override public <T, S> GridCacheAtomicStamped<T, S> atomicStamped(String name, T initVal, S initStamp,
        boolean create) throws GridException {
        return dsMgr.atomicStamped(name, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicStamped(String name) throws GridException {
        return dsMgr.removeAtomicStamped(name);
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheQueue<T> queue(String name, int cap, boolean collocated, boolean create)
        throws GridException {
        return dsMgr.queue(name, cap <= 0 ? Integer.MAX_VALUE : cap, collocated, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name) throws GridException {
        return dsMgr.removeQueue(name, 0);
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name, int batchSize) throws GridException {
        return dsMgr.removeQueue(name, batchSize);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> GridCacheSet<T> set(String name, boolean collocated, boolean create)
        throws GridException {
        return dsMgr.set(name, collocated, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeSet(String name) throws GridException {
        return dsMgr.removeSet(name);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws GridException {
        return dsMgr.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeCountDownLatch(String name) throws GridException {
        return dsMgr.removeCountDownLatch(name);
    }
}
