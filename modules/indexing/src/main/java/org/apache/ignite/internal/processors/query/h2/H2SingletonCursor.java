package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

import java.util.NoSuchElementException;

/** FIXME */
public class H2SingletonCursor  implements Cursor {

    /** FIXME */
    private final GridH2Row row;
    /** FIXME */
    private boolean isBeforeFirstRow;

    /** FIXME */
    public H2SingletonCursor(GridH2Row row) {
        this.row = row;
        this.isBeforeFirstRow = true;
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        if (isBeforeFirstRow)
            DbException.convert(new NoSuchElementException("next() has to be called before get()"));

        return row;
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        if (!isBeforeFirstRow)
            return false;

        isBeforeFirstRow = false;
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        if (isBeforeFirstRow)
            return false;

        isBeforeFirstRow = true;
        return true;
    }
}
