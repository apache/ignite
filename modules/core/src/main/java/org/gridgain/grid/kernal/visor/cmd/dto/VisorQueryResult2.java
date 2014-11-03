/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Result for cache query tasks.
 */
public class VisorQueryResult2 extends VisorQueryResult {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query duration */
    private final long duration;

    /**
     * Create task result with given parameters
     *
     * @param rows Rows fetched from query.
     * @param hasMore Whether query has more rows to fetch.
     */
    public VisorQueryResult2(List<Object[]> rows, Boolean hasMore, long duration) {
        super(rows, hasMore);

        this.duration = duration;
    }

    public VisorQueryResult2(VisorQueryResult res, long duration) {
        this(res.rows(), res.hasMore(), duration);
    }

    /**
     * @return Duration of next page fetching.
     */
    public long duratuin() {
        return duration;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryResult2.class, this);
    }
}
