/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.lang.*;

/**
 * Wrapper used to covert {@link org.gridgain.grid.spi.IgniteSpiCloseableIterator} to {@link GridCloseableIterator}.
 */
public class GridSpiCloseableIteratorWrapper<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteSpiCloseableIterator<T> iter;

    /**
     * @param iter Spi iterator.
     */
    public GridSpiCloseableIteratorWrapper(IgniteSpiCloseableIterator<T> iter) {
        assert iter != null;

        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override protected T onNext() throws GridException {
        return iter.next();
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws GridException {
        return iter.hasNext();
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws GridException {
        iter.close();
    }
}
