/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.*;
import org.h2.index.*;
import org.h2.result.*;
import org.h2.table.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Unsorted merge index.
 */
public class GridMergeIndexUnsorted extends GridMergeIndex {
    /** */
    private final BlockingQueue<GridResultPage<?>> queue = new LinkedBlockingQueue<>();

    /**
     * @param tbl  Table.
     * @param name Index name.
     */
    public GridMergeIndexUnsorted(GridMergeTable tbl, String name) {
        super(tbl, name, IndexType.createScan(false), IndexColumn.wrap(tbl.getColumns()));
    }

    /** {@inheritDoc} */
    @Override public void addPage0(GridResultPage<?> page) {
        queue.add(page);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
        return new FetchingCursor(new Iterator<Row>() {
            /** */
            Iterator<Value[]> iter = Collections.emptyIterator();

            @Override public boolean hasNext() {
                if (iter.hasNext())
                    return true;

                GridResultPage<?> page;

                try {
                    page = queue.take();
                }
                catch (InterruptedException e) {
                    throw new IgniteException("Query execution was interrupted.", e);
                }

                if (page == END) {
                    assert queue.isEmpty() : "It must be the last page: " + queue;

                    return false; // We are done.
                }

                page.fetchNextPage();

                iter = page.response().rows().iterator();

                assert iter.hasNext();

                return true;
            }

            @Override public Row next() {
                return new Row(iter.next(), 0);
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }
}
