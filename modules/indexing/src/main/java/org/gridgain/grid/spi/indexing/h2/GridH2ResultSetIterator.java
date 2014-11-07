/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;


/**
 * Iterator over result set.
 */
abstract class GridH2ResultSetIterator<T> implements GridSpiCloseableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Object[][] data;

    /** */
    private int idx;

    /**
     * @param data Data array.
     */
    protected GridH2ResultSetIterator(Object[][] data) {
        assert data != null;

        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return idx < data.length && data[idx] != null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
    @Override public T next() {
        return createRow(data[idx++]);
    }

    /**
     * @param row Row source.
     * @return Row.
     */
    protected abstract T createRow(Object[] row);

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        data = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString((Class<GridH2ResultSetIterator>)getClass(), this);
    }
}
