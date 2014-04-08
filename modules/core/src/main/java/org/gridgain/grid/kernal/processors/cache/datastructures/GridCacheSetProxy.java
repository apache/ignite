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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Cache set proxy.
 */
public class GridCacheSetProxy<T> implements GridCacheSet<T> {
    /** Delegate set. */
    private GridCacheSetImpl<T> delegate;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Cache gateway. */
    private final GridCacheGateway gate;

    /**
     * @param cctx Cache context.
     * @param delegate Delegate set.
     */
    public GridCacheSetProxy(GridCacheContext cctx, GridCacheSetImpl<T> delegate) {
        this.cctx = cctx;
        this.delegate = delegate;

        gate = cctx.gate();
    }

    /**
     * @return Delegate set.
     */
    GridCacheSetImpl<T> delegate() {
        return delegate;
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(final Object o) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T1> T1[] toArray(final T1[] a) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(final T t) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final Object o) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(final Collection<?> c) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(final Collection<? extends T> c) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(final Collection<?> c) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(final Collection<?> c) {
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
        catch (GridException e) {
            throw new GridRuntimeException(e);
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
                    @Override
                    public Void call() throws Exception {
                        delegate.clear();

                        return null;
                    }
                }, cctx);
            }
            else
                delegate.clear();
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    @Override public GridCloseableIterator<T> iteratorEx() {
        gate.enter();

        try {
            if (cctx.transactional())
                return CU.outTx(new Callable<GridCloseableIterator<T>>() {
                    @Override public GridCloseableIterator<T> call() throws Exception {
                        return delegate.iteratorEx();
                    }
                }, cctx);

            return delegate.iteratorEx();
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
        finally {
            gate.leave();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Method iterator() is not supported for GridCacheSet, " +
            "use iteratorEx() instead.");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() throws GridException {
        return delegate.collocated();
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return delegate.removed();
    }
}
