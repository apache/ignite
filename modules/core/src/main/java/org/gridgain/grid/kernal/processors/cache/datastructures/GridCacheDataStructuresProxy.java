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

import java.io.*;

/**
 * Data structures proxy object.
 */
public class GridCacheDataStructuresProxy<K, V> implements GridCacheDataStructures, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Delegate object. */
    private GridCacheDataStructures delegate;

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheDataStructuresProxy() {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param delegate Delegate object.
     */
    public GridCacheDataStructuresProxy(GridCacheContext<K, V> cctx, GridCacheDataStructures delegate) {
        this.delegate = delegate;
        this.cctx = cctx;

        gate = cctx.gate();
    }

    /** {@inheritDoc} */
    @Override public GridCacheAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicSequence(name, initVal, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicSequence(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicSequence(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheAtomicLong atomicLong(String name, long initVal, boolean create) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicLong(name, initVal, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicLong(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicLong(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheAtomicReference<T> atomicReference(String name, T initVal, boolean create)
        throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicReference(name, initVal, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicReference(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicReference(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, S> GridCacheAtomicStamped<T, S> atomicStamped(String name, T initVal, S initStamp,
        boolean create) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.atomicStamped(name, initVal, initStamp, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicStamped(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeAtomicStamped(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheQueue<T> queue(String name, int cap, boolean collocated, boolean create)
        throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.queue(name, cap, collocated, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeQueue(name, 0);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name, int batchSize) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeQueue(name, batchSize);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> GridCacheSet<T> set(String name, boolean collocated, boolean create)
        throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.set(name, collocated, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeSet(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeSet(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.countDownLatch(name, cnt, autoDel, create);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeCountDownLatch(String name) throws GridException {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.removeCountDownLatch(name);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cctx = (GridCacheContext<K, V>)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return cctx.grid().cache(cctx.cache().name()).dataStructures();
    }
}
