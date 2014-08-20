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
public class VisorQueryResultEx extends VisorQueryResult {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node where query executed. */
    private final UUID resNodeId;

    /** Query ID to store in node local. */
    private final String qryId;

    /** Query columns descriptors.  */
    private final VisorFieldsQueryColumn[] colNames;

    /**
     * @param resNodeId Node where query executed.
     * @param qryId Query ID for future extraction in nextPage() access.
     * @param colNames Column type and names.
     * @param rows Rows fetched from query.
     * @param hasMore Whether query has more rows to fetch.
     */
    public VisorQueryResultEx(
        UUID resNodeId,
        String qryId,
        VisorFieldsQueryColumn[] colNames,
        List<Object[]> rows,
        Boolean hasMore
    ) {
        super(rows, hasMore);

        this.resNodeId = resNodeId;
        this.qryId = qryId;
        this.colNames = colNames;
    }

    /**
     * @return Response node id.
     */
    public UUID responseNodeId() {
        return resNodeId;
    }

    /**
     * @return Query id.
     */
    public String queryId() {
        return qryId;
    }

    /**
     * @return Column names.
     */
    public VisorFieldsQueryColumn[] columnNames() {
        return colNames;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryResultEx.class, this);
    }
}
