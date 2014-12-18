/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep;

import org.h2.result.*;
import org.h2.value.*;

import java.util.*;

/**
 * Page result.
 */
public abstract class GridResultPage<S> {
    /** */
    private final S src;

    /** */
    private final Collection<Value[]> rows;

    /** */
    private final int page;

    /**
     * @param src Source.
     * @param page Page.
     * @param rows Page rows.
     */
    protected GridResultPage(S src, int page, Collection<Value[]> rows) {
        assert src != null;
        assert rows != null;

        this.src = src;
        this.page = page;
        this.rows = rows;
    }

    /**
     * @return Result source.
     */
    public S source() {
        return src;
    }

    /**
     * @return Page.
     */
    public int page() {
        return page;
    }

    /**
     * @return {@code true} If result is empty.
     */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /**
     * @return Page rows.
     */
    public Collection<Value[]> rows() {
        return rows;
    }

    /**
     * Request next page.
     */
    public abstract void fetchNextPage();
}
