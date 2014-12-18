/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep;

import org.apache.ignite.*;
import org.h2.index.*;
import org.h2.result.*;
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

    /** {@inheritDoc} */
    @Override public void addPage(GridResultPage<?> page) {
        queue.add(page);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
        final GridResultPage<?> p = queue.poll();

        assert p != null; // First page must be already fetched.

        if (p.isEmpty())
            return new IteratorCursor(Collections.<Row>emptyIterator());

        p.fetchNextPage(); // We always request next page before reading this one.

        return new FetchingCursor() {
            /** */
            Iterator<Value[]> iter = p.rows().iterator();

            @Nullable @Override protected Row fetchNext() {
                if (!iter.hasNext()) {
                    GridResultPage<?> page;

                    try {
                        page = queue.take();
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException("Query execution was interrupted.", e);
                    }

                    if (page.isEmpty()) {
                        assert queue.isEmpty() : "It must be the last page.";

                        return null; // Empty page - we are done.
                    }

                    page.fetchNextPage();

                    iter = page.rows().iterator();
                }

                return new Row(iter.next(), 0);
            }
        };
    }
}
