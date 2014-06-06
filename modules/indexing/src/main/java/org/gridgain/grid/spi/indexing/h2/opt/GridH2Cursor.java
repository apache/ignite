/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;

import java.util.*;

/**
 * H2 Cursor implementation.
 */
public class GridH2Cursor implements Cursor {
    /** */
    private Iterator<GridH2Row> iter;

    /** */
    private Row row;

    /**
     * Constructor.
     *
     * @param iter Rows iterator.
     */
    public GridH2Cursor(Iterator<GridH2Row> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        row = null;

        if (iter.hasNext())
            row = iter.next();

        return row != null;
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        // Should never be called.
        throw DbException.getUnsupportedException("previous");
    }
}
