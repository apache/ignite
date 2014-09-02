/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.util.typedef.internal.S;

import java.util.List;
import java.util.UUID;

/**
 * Result for cache query tasks 2.
 *
 * TODO GG-8942
 * @deprecated Should be merged with VisorQueryResultEx in compatibility breaking release.
 */
public class VisorQueryResultEx2 extends VisorQueryResultEx {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query duration */
    private final long duration;

    /**
     * @param resNodeId Node where query executed.
     * @param qryId Query ID for future extraction in nextPage() access.
     * @param colNames Columns types and names.
     * @param rows Rows fetched from query.
     * @param hasMore Whether query has more rows to fetch.
     * @param duration Query duration.
     */
    public VisorQueryResultEx2(
            UUID resNodeId,
            String qryId,
            VisorFieldsQueryColumn[] colNames,
            List<Object[]> rows,
            Boolean hasMore,
            long duration
    ) {
        super(resNodeId, qryId, colNames, rows, hasMore);

        this.duration = duration;
    }

    /**
     * @return Query duration
     */
    public long duration() {
        return duration;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryResultEx2.class, this);
    }
}
