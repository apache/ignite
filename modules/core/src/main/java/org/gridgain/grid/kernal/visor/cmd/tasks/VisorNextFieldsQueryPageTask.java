/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorFieldsQueryTask.*;
import org.gridgain.grid.lang.*;

import java.util.*;

import static org.gridgain.grid.kernal.visor.cmd.tasks.VisorFieldsQueryUtils.*;

/**
 *  Task for collecting next page for previously executed SQL or SCAN query.
 */
@GridInternal
public class VisorNextFieldsQueryPageTask extends VisorOneNodeTask<VisorNextFieldsQueryPageTask.VisorNextFieldsQueryPageArg,
    VisorFieldsQueryResult> {
    /**
     * Arguments for {@link VisorNextFieldsQueryPageTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorNextFieldsQueryPageArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        private final String qryId;

        private final Integer pageSize;

        /**
         * @param nodeId Node Id.
         * @param qryId Query ID for future extraction in nextPage() access.
         * @param pageSize Results page size.
         */
        public VisorNextFieldsQueryPageArg(UUID nodeId, String qryId, Integer pageSize) {
            super(nodeId);

            this.qryId = qryId;
            this.pageSize = pageSize;
        }
    }

    private static class VisorNextFieldsQueryPageJob
        extends VisorOneNodeJob<VisorNextFieldsQueryPageArg, VisorFieldsQueryResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        private VisorNextFieldsQueryPageJob(VisorNextFieldsQueryPageArg arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected VisorFieldsQueryResult run(VisorNextFieldsQueryPageArg arg) throws GridException {
            return arg.qryId.startsWith(SCAN_QRY_NAME) ? nextScanPage(arg) : nextSqlPage(arg);
        }

        /** Collect data from SQL query */
        private VisorFieldsQueryResult nextSqlPage(VisorNextFieldsQueryPageArg arg) throws GridException {
            GridNodeLocalMap<String, VisorFutureResultSetHolder<List<?>>> storage = g.nodeLocalMap();

            VisorFutureResultSetHolder<List<?>> t = storage.get(arg.qryId);

            if (t == null)
                throw new GridInternalException("SQL query results are expired.");

            GridBiTuple<List<Object[]>, List<?>> nextRows = fetchSqlQueryRows(t.future(), t.next(), arg.pageSize);

            boolean hasMore = nextRows.get2() != null;

            if (hasMore)
                storage.put(arg.qryId, new VisorFutureResultSetHolder<>(t.future(), nextRows.get2(), true));
            else
                storage.remove(arg.qryId);

            return new VisorFieldsQueryResult (nextRows.get1(), hasMore);
        }

        /** Collect data from SCAN query */
        private VisorFieldsQueryResult nextScanPage(VisorNextFieldsQueryPageArg arg) throws GridException {
            GridNodeLocalMap<String, VisorFutureResultSetHolder<Map.Entry<Object, Object>>> storage = g.nodeLocalMap();

            VisorFutureResultSetHolder<Map.Entry<Object, Object>> t = storage.get(arg.qryId);

            if (t == null)
                throw new GridInternalException("Scan query results are expired.");

            GridBiTuple<List<Object[]>, Map.Entry<Object, Object>> nextRows = fetchScanQueryRows(t.future(), t.next(), arg.pageSize);

            Boolean hasMore = nextRows.get2() != null;

            if (hasMore)
                storage.put(arg.qryId, new VisorFutureResultSetHolder<>(t.future(), nextRows.get2(), true));
            else
                storage.remove(arg.qryId);

            return new VisorFieldsQueryResult(nextRows.get1(), hasMore);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorNextFieldsQueryPageJob job(VisorNextFieldsQueryPageArg arg) {
        return new VisorNextFieldsQueryPageJob(arg);
    }
}
