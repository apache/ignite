package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.query.h2.opt.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;

/**
 * Cursor.
 */
public class H2Cursor implements Cursor {
    /** */
    final GridCursor<GridH2Row> cursor;

    /** */
    final IgniteBiPredicate<Object,Object> filter;

    /** */
    final long time = U.currentTimeMillis();

    /**
     * @param cursor Cursor.
     * @param filter Filter.
     */
    public H2Cursor(GridCursor<GridH2Row> cursor, IgniteBiPredicate<Object, Object> filter) {
        assert cursor != null;

        this.cursor = cursor;
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        try {
            return cursor.get();
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        try {
            while (cursor.next()) {
                GridH2Row row = cursor.get();

                if (row.expireTime() > 0 && row.expireTime() <= time)
                    continue;

                if (filter == null)
                    return true;

                Object key = row.getValue(0).getObject();
                Object val = row.getValue(1).getObject();

                assert key != null;
                assert val != null;

                if (filter.apply(key, val))
                    return true;
            }

            return false;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        throw DbException.getUnsupportedException("previous");
    }
}
