/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.GridNodeLocalMap;
import org.gridgain.grid.kernal.GridInternalException;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.VisorJob;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeArg;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeJob;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeTask;
import org.gridgain.grid.lang.GridBiTuple;
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorFieldsQueryTask.*;

import static org.gridgain.grid.kernal.visor.cmd.tasks.VisorFieldsQueryUtils.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *  Task for collecting next page for previously executed SQL or SCAN query.
 */
@GridInternal
public class VisorNextFieldsQueryPageTask extends VisorOneNodeTask<
    VisorNextFieldsQueryPageTask.VisorNextFieldsQueryPageArg,
    VisorNextFieldsQueryPageTask.VisorNextFieldsQueryPageTaskResult> {
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

    @SuppressWarnings("PublicInnerClass")
    public static class VisorNextFieldsQueryPageTaskResult implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final List<Object[]> rows;

        private final Boolean hasMore;

        public VisorNextFieldsQueryPageTaskResult(List<Object[]> rows, Boolean hasMore) {
            this.rows = rows;
            this.hasMore = hasMore;
        }

        /**
         * @return Rows.
         */
        public List<Object[]> rows() {
            return rows;
        }

        /**
         * @return Has more.
         */
        public Boolean hasMore() {
            return hasMore;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorNextFieldsQueryPageJob
        extends VisorOneNodeJob<VisorNextFieldsQueryPageArg, VisorNextFieldsQueryPageTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorNextFieldsQueryPageJob(VisorNextFieldsQueryPageArg arg) {
            super(arg);
        }

        @Override
        protected VisorNextFieldsQueryPageTaskResult run(VisorNextFieldsQueryPageArg arg) throws GridException {
            return arg.qryId.startsWith(SCAN_QRY_NAME) ? nextScanPage(arg) : nextSqlPage(arg);
        }

        /** Collect data from SQL query */
        private VisorNextFieldsQueryPageTaskResult nextSqlPage(VisorNextFieldsQueryPageArg arg) throws GridException {
            GridNodeLocalMap<String, VisorSqlStorageValType> storage = g.nodeLocalMap();

            VisorSqlStorageValType t = storage.get(arg.qryId);

            if (t == null)
                throw new GridInternalException("SQL query results are expired.");

            GridBiTuple<List<Object[]>, List<?>> nextRows = fetchSqlQueryRows(t.future(), t.next(), arg.pageSize);

            boolean hasMore = nextRows.get2() != null;

            if (hasMore)
                storage.put(arg.qryId, new VisorSqlStorageValType(t.future(), nextRows.get2(), true));
            else
                storage.remove(arg.qryId);

            return new VisorNextFieldsQueryPageTaskResult (nextRows.get1(), hasMore);
        }

        /** Collect data from SCAN query */
        private VisorNextFieldsQueryPageTaskResult nextScanPage(VisorNextFieldsQueryPageArg arg) throws GridException {
            GridNodeLocalMap<String, VisorScanStorageValType> storage = g.nodeLocalMap();

            VisorScanStorageValType t = storage.get(arg.qryId);

            if (t == null)
                throw new GridInternalException("Scan query results are expired.");

            GridBiTuple<List<Object[]>, Map.Entry<Object, Object>> nextRows = fetchScanQueryRows(t.future(), t.next(), arg.pageSize);

            Boolean hasMore = nextRows.get2() != null;

            if (hasMore)
                storage.put(arg.qryId, new VisorScanStorageValType(t.future(), nextRows.get2(), true));
            else
                storage.remove(arg.qryId);

            return new VisorNextFieldsQueryPageTaskResult(nextRows.get1(), hasMore);
        }
    }

    @Override
    protected VisorJob<VisorNextFieldsQueryPageArg, VisorNextFieldsQueryPageTaskResult> job(
        VisorNextFieldsQueryPageArg arg) {
        return new VisorNextFieldsQueryPageJob(arg);
    }
}
