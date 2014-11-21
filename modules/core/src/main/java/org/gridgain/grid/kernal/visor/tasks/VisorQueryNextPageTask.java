/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.dto.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 *  Task for collecting next page previously executed SQL or SCAN query.
 */
@GridInternal
public class VisorQueryNextPageTask extends VisorOneNodeTask<GridBiTuple<String, Integer>, VisorQueryResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryNextPageJob job(GridBiTuple<String, Integer> arg) {
        return new VisorQueryNextPageJob(arg);
    }

    /**
     * Job for collecting next page previously executed SQL or SCAN query.
     */
    private static class VisorQueryNextPageJob extends VisorJob<GridBiTuple<String, Integer>, VisorQueryResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        private VisorQueryNextPageJob(GridBiTuple<String, Integer> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected VisorQueryResult run(GridBiTuple<String, Integer> arg) throws GridException {
            return arg.get1().startsWith(VisorQueryUtils.SCAN_QRY_NAME) ? nextScanPage(arg) : nextSqlPage(arg);
        }

        /** Collect data from SQL query */
        private VisorQueryResult nextSqlPage(GridBiTuple<String, Integer> arg) throws GridException {
            long start = U.currentTimeMillis();

            GridNodeLocalMap<String, VisorQueryTask.VisorFutureResultSetHolder<List<?>>> storage = g.nodeLocalMap();

            VisorQueryTask.VisorFutureResultSetHolder<List<?>> t = storage.get(arg.get1());

            if (t == null)
                throw new GridInternalException("SQL query results are expired.");

            GridBiTuple<List<Object[]>, List<?>> nextRows = VisorQueryUtils.fetchSqlQueryRows(t.future(), t.next(), arg.get2());

            boolean hasMore = nextRows.get2() != null;

            if (hasMore)
                storage.put(arg.get1(), new VisorQueryTask.VisorFutureResultSetHolder<>(t.future(), nextRows.get2(), true));
            else
                storage.remove(arg.get1());

            return new VisorQueryResult(nextRows.get1(), hasMore, U.currentTimeMillis() - start);
        }

        /** Collect data from SCAN query */
        private VisorQueryResult nextScanPage(GridBiTuple<String, Integer> arg) throws GridException {
            long start = U.currentTimeMillis();

            GridNodeLocalMap<String, VisorQueryTask.VisorFutureResultSetHolder<Map.Entry<Object, Object>>> storage = g.nodeLocalMap();

            VisorQueryTask.VisorFutureResultSetHolder<Map.Entry<Object, Object>> t = storage.get(arg.get1());

            if (t == null)
                throw new GridInternalException("Scan query results are expired.");

            GridBiTuple<List<Object[]>, Map.Entry<Object, Object>> rows =
                VisorQueryUtils.fetchScanQueryRows(t.future(), t.next(), arg.get2());

            Boolean hasMore = rows.get2() != null;

            if (hasMore)
                storage.put(arg.get1(), new VisorQueryTask.VisorFutureResultSetHolder<>(t.future(), rows.get2(), true));
            else
                storage.remove(arg.get1());

            return new VisorQueryResult(rows.get1(), hasMore, U.currentTimeMillis() - start);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryNextPageJob.class, this);
        }
    }
}
