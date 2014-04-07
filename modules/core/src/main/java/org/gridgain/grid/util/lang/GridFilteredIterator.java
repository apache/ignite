/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import java.util.*;

/**
 * Filtered iterator.
 */
public abstract class GridFilteredIterator<T> implements Iterator<T> {
    /** */
    private final Iterator<? extends T> it;

    /** */
    private boolean hasNext;

    /** */
    private T next;

    /**
     * @param it Iterator.
     */
    public GridFilteredIterator(Iterator<? extends T> it) {
        assert it != null;

        this.it = it;
    }

    /**
     * @param t The object.
     * @return {@code true} If the object is accepted by the filter.
     */
    protected abstract boolean accept(T t);

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (hasNext)
            return true;

        while (it.hasNext()) {
            T t = it.next();

            if (accept(t)) {
                next = t;
                hasNext = true;

                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        if (!hasNext())
            throw new NoSuchElementException();

        T res = next;

        next = null;
        hasNext = false;

        return res;
    }


    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
