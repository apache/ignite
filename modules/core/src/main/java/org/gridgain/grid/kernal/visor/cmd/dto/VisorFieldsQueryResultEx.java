/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.kernal.visor.cmd.dto.node.*;
import org.gridgain.grid.kernal.visor.cmd.tasks.*;

import java.util.*;

/**
 * Result of {@link VisorFieldsQueryTask}.
 */
public class VisorFieldsQueryResultEx extends VisorFieldsQueryResult {
    /** */
    private static final long serialVersionUID = 0L;

    private final UUID resNodeId;

    private final String qryId;

    private final VisorFieldsQueryColumn[] colNames;

    /**
     * @param resNodeId Node where query executed.
     * @param qryId Query ID for future extraction in nextPage() access.
     * @param colNames Column type and names.
     * @param rows First page of rows fetched from query.
     * @param hasMore `True` if there is more data could be fetched.
     */
    public VisorFieldsQueryResultEx(UUID resNodeId, String qryId, VisorFieldsQueryColumn[] colNames,
        List<Object[]> rows, Boolean hasMore) {
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
}
