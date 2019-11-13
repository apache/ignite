/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.action.annotation.ActionController;
import org.apache.ignite.agent.action.query.CursorHolder;
import org.apache.ignite.agent.action.query.QueryHolder;
import org.apache.ignite.agent.action.query.QueryHolderRegistry;
import org.apache.ignite.agent.dto.action.query.NextPageQueryArgument;
import org.apache.ignite.agent.dto.action.query.QueryArgument;
import org.apache.ignite.agent.dto.action.query.QueryResult;
import org.apache.ignite.agent.dto.action.query.ScanQueryArgument;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.agent.utils.QueryUtils.fetchResult;
import static org.apache.ignite.agent.utils.QueryUtils.fetchScanQueryResult;
import static org.apache.ignite.agent.utils.QueryUtils.fetchSqlQueryResult;
import static org.apache.ignite.agent.utils.QueryUtils.prepareQuery;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;

/**
 * Query actions controller.
 */
@ActionController("QueryActions")
public class QueryActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /** Query registry. */
    private final QueryHolderRegistry qryRegistry;

    /** Query process. */
    private GridQueryProcessor qryProc;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public QueryActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(QueryActionsController.class);
        this.qryProc = ctx.query();
        this.qryRegistry = new QueryHolderRegistry(ctx, Duration.ofMinutes(5).toMillis());
    }

    /**
     * Cancel query by query ID.
     *
     * @param qryId Query id.
     * @return Completable feature.
     */
    public IgniteFuture<Void> cancel(String qryId) {
        qryRegistry.cancelQuery(qryId);

        return new IgniteFinishedFutureImpl<>();
    }

    /**
     * @param arg Argument.
     * @return Next page with result.
     */
    public IgniteInternalFuture<QueryResult> nextPage(NextPageQueryArgument arg) {
        String qryId = requireNonNull(arg.getQueryId(), "Failed to execute query due to empty query ID");
        String cursorId = requireNonNull(arg.getCursorId(), "Failed to execute query due to empty cursor ID");

        return ctx.closure().callLocalSafe(() -> {
            CursorHolder cursorHolder = qryRegistry.findCursor(qryId, cursorId);

            QueryResult res = fetchResult(cursorHolder, arg.getPageSize());

            res.setResultNodeId(ctx.localNodeId().toString());

            if (!res.isHasMore())
                qryRegistry.closeQueryCursor(qryId, cursorId);

            return res;
        }, MANAGEMENT_POOL);
    }

    /**
     * @param arg Argument.
     * @return List of query results.
     */
    public IgniteInternalFuture<List<QueryResult>> executeSqlQuery(QueryArgument arg) {
        String qryId = requireNonNull(arg.getQueryId(), "Failed to execute query due to empty query ID");

        return ctx.closure().callLocalSafe(() -> {
            qryRegistry.cancelQuery(qryId);

            List<QueryResult> results = new ArrayList<>();
            QueryHolder qryHolder = qryRegistry.createQueryHolder(qryId);

            try {
                if (log.isDebugEnabled())
                    log.debug("Execute query started with subject: " + ctx.security().securityContext().subject());

                SqlFieldsQuery qry = prepareQuery(arg);

                GridCacheContext cctx = F.isEmpty(arg.getCacheName())
                        ? null
                        : ctx.cache().cache(arg.getCacheName()).context();

                for (FieldsQueryCursor<List<?>> cur : qryProc.querySqlFields(cctx, qry, null, true, false, qryHolder.cancelHook())) {
                    CursorHolder cursorHolder = new CursorHolder(cur);

                    QueryResult res = fetchSqlQueryResult(cursorHolder, arg.getPageSize());

                    res.setResultNodeId(ctx.localNodeId().toString());

                    if (res.isHasMore())
                        res.setCursorId(qryRegistry.addCursor(qryId, cursorHolder));

                    results.add(res);
                }

                return results;
            }
            catch (Throwable e) {
                log.warning("Failed to execute query.", e);

                qryRegistry.cancelQuery(qryId);

                throw e;
            }
        }, MANAGEMENT_POOL);
    }

    /**
     * @param arg Argument.
     * @return List of query results.
     */
    public IgniteInternalFuture<List<QueryResult>> executeScanQuery(ScanQueryArgument arg) {
        String qryId = requireNonNull(arg.getQueryId(), "Failed to execute query due to empty query ID");
        String cacheName = requireNonNull(arg.getCacheName(), "Failed to execute query due to empty cache name");

        return ctx.closure().callLocalSafe(() -> {
            qryRegistry.cancelQuery(qryId);
            qryRegistry.createQueryHolder(qryId);

            try {
                ScanQuery<Object, Object> qry = new ScanQuery<>()
                        .setPageSize(arg.getPageSize())
                        .setLocal(arg.getTargetNodeId() != null);

                IgniteCache<Object, Object> c = ctx.grid().cache(cacheName);

                CursorHolder cursorHolder = new CursorHolder(c.withKeepBinary().query(qry), true);

                QueryResult res = fetchScanQueryResult(cursorHolder, arg.getPageSize());

                res.setResultNodeId(ctx.localNodeId().toString());

                if (res.isHasMore())
                    res.setCursorId(qryRegistry.addCursor(qryId, cursorHolder));

                return Collections.singletonList(res);
            }
            catch (Throwable e) {
                log.warning("Failed to execute scan query: [qryId=" + qryId + ", cache=" + cacheName + ']', e);

                qryRegistry.cancelQuery(qryId);

                throw e;
            }
        });
    }
}
