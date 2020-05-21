/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.dml.DmlAstUtils;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.sql.SqlParseException;
import org.apache.ignite.internal.sql.SqlParser;
import org.apache.ignite.internal.sql.SqlStrictParseException;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlKillComputeTaskCommand;
import org.apache.ignite.internal.sql.command.SqlKillContinuousQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillScanQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillServiceCommand;
import org.apache.ignite.internal.sql.command.SqlKillTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter.keyColumn;

/**
 * Parser module. Splits incoming request into a series of parsed results.
 */
public class QueryParser {
    /** */
    private static final int CACHE_SIZE = 1024;

    /** A pattern for commands having internal implementation in Ignite. */
    private static final Pattern INTERNAL_CMD_RE = Pattern.compile(
        "^(create|drop)\\s+index|^alter\\s+table|^copy|^set|^begin|^commit|^rollback|^(create|alter|drop)\\s+user" +
            "|^kill\\s+(query|scan|continuous|compute|service|transaction)|show|help|grant|revoke",
        Pattern.CASE_INSENSITIVE);

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Logger. */
    private final IgniteLogger log;

    /** Query parser metrics holder. */
    private final QueryParserMetricsHolder metricsHolder;

    /** */
    private volatile GridBoundedConcurrentLinkedHashMap<QueryDescriptor, QueryParserCacheEntry> cache =
        new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);

    /**
     * Constructor.
     *
     * @param idx Indexing instance.
     * @param connMgr Connection manager.
     */
    public QueryParser(IgniteH2Indexing idx, ConnectionManager connMgr) {
        this.idx = idx;
        this.connMgr = connMgr;

        this.log = idx.kernalContext().log(QueryParser.class);
        this.metricsHolder = new QueryParserMetricsHolder(idx.kernalContext().metric());
    }

    /**
     * Parse the query.
     *
     * @param schemaName schema name.
     * @param qry query to parse.
     * @param remainingAllowed Whether multiple statements are allowed.
     * @return Parsing result that contains Parsed leading query and remaining sql script.
     */
    public QueryParserResult parse(String schemaName, SqlFieldsQuery qry, boolean remainingAllowed) {
        QueryParserResult res = parse0(schemaName, qry, remainingAllowed);

        checkQueryType(qry, res.isSelect());

        return res;
    }

    /**
     * Create parameters from query.
     *
     * @param qry Query.
     * @return Parameters.
     */
    public QueryParameters queryParameters(SqlFieldsQuery qry) {
        NestedTxMode nestedTxMode = NestedTxMode.DEFAULT;
        boolean autoCommit = true;
        List<Object[]> batchedArgs = null;

        if (qry instanceof SqlFieldsQueryEx) {
            SqlFieldsQueryEx qry0 = (SqlFieldsQueryEx)qry;

            if (qry0.getNestedTxMode() != null)
                nestedTxMode = qry0.getNestedTxMode();

            autoCommit = qry0.isAutoCommit();

            batchedArgs = qry0.batchedArguments();
        }

        int timeout;

        if (qry.getTimeout() >= 0)
            timeout = qry.getTimeout();
        else
            timeout = (int)idx.kernalContext().config().getSqlConfiguration().getDefaultQueryTimeout();

        return new QueryParameters(
            qry.getArgs(),
            qry.getPartitions(),
            timeout,
            qry.isLazy(),
            qry.getPageSize(),
            null,
            nestedTxMode,
            autoCommit,
            batchedArgs,
            qry.getUpdateBatchSize()
        );
    }

    /**
     * Parse the query.
     *
     * @param schemaName schema name.
     * @param qry query to parse.
     * @param remainingAllowed Whether multiple statements are allowed.
     * @return Parsing result that contains Parsed leading query and remaining sql script.
     */
    private QueryParserResult parse0(String schemaName, SqlFieldsQuery qry, boolean remainingAllowed) {
        QueryDescriptor qryDesc = queryDescriptor(schemaName, qry);

        QueryParserCacheEntry cached = cache.get(qryDesc);

        if (cached != null) {
            metricsHolder.countCacheHit();

            return new QueryParserResult(
                qryDesc,
                queryParameters(qry),
                null,
                cached.parametersMeta(),
                cached.select(),
                cached.dml(),
                cached.command()
            );
        }

        metricsHolder.countCacheMiss();

        // Try parsing as native command.
        QueryParserResult parseRes = parseNative(schemaName, qry, remainingAllowed);

        // Otherwise parse with H2.
        if (parseRes == null)
            parseRes = parseH2(schemaName, qry, qryDesc.batched(), remainingAllowed);

        // Add to cache if not multi-statement.
        if (parseRes.remainingQuery() == null) {
            cached = new QueryParserCacheEntry(parseRes.parametersMeta(), parseRes.select(), parseRes.dml(), parseRes.command());

            cache.put(qryDesc, cached);
        }

        // Done.
        return parseRes;
    }

    /**
     * Tries to parse sql query text using native parser. Only first (leading) sql command of the multi-statement is
     * actually parsed.
     *
     * @param schemaName Schema name.
     * @param qry which sql text to parse.
     * @param remainingAllowed Whether multiple statements are allowed.
     * @return Command or {@code null} if cannot parse this query.
     */
    @SuppressWarnings("IfMayBeConditional")
    private @Nullable QueryParserResult parseNative(String schemaName, SqlFieldsQuery qry, boolean remainingAllowed) {
        String sql = qry.getSql();

        // Heuristic check for fast return.
        if (!INTERNAL_CMD_RE.matcher(sql.trim()).find())
            return null;

        try {
            SqlParser parser = new SqlParser(schemaName, sql);

            SqlCommand nativeCmd = parser.nextCommand();

            assert nativeCmd != null : "Empty query. Parser met end of data";

            if (!(nativeCmd instanceof SqlCreateIndexCommand
                || nativeCmd instanceof SqlDropIndexCommand
                || nativeCmd instanceof SqlBeginTransactionCommand
                || nativeCmd instanceof SqlCommitTransactionCommand
                || nativeCmd instanceof SqlRollbackTransactionCommand
                || nativeCmd instanceof SqlBulkLoadCommand
                || nativeCmd instanceof SqlAlterTableCommand
                || nativeCmd instanceof SqlSetStreamingCommand
                || nativeCmd instanceof SqlCreateUserCommand
                || nativeCmd instanceof SqlAlterUserCommand
                || nativeCmd instanceof SqlDropUserCommand
                || nativeCmd instanceof SqlKillQueryCommand
                || nativeCmd instanceof SqlKillComputeTaskCommand
                || nativeCmd instanceof SqlKillServiceCommand
                || nativeCmd instanceof SqlKillTransactionCommand
                || nativeCmd instanceof SqlKillScanQueryCommand
                || nativeCmd instanceof SqlKillContinuousQueryCommand)
            )
                return null;

            SqlFieldsQuery newQry = cloneFieldsQuery(qry).setSql(parser.lastCommandSql());

            QueryDescriptor newPlanKey = queryDescriptor(schemaName, newQry);

            SqlFieldsQuery remainingQry = null;

            if (!F.isEmpty(parser.remainingSql())) {
                checkRemainingAllowed(remainingAllowed);

                remainingQry = cloneFieldsQuery(qry).setSql(parser.remainingSql()).setArgs(qry.getArgs());
            }

            QueryParserResultCommand cmd = new QueryParserResultCommand(nativeCmd, null, false);

            return new QueryParserResult(
                newPlanKey,
                queryParameters(newQry),
                remainingQry,
                Collections.emptyList(), // Currently none of native statements supports parameters.
                null,
                null,
                cmd
            );
        }
        catch (SqlStrictParseException e) {
            throw new IgniteSQLException(e.getMessage(), e.errorCode(), e);
        }
        catch (Exception e) {
            // Cannot parse, return.
            if (log.isDebugEnabled())
                log.debug("Failed to parse SQL with native parser [qry=" + sql + ", err=" + e + ']');

            if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK))
                return null;

            int code = IgniteQueryErrorCode.PARSING;

            if (e instanceof SqlParseException)
                code = ((SqlParseException)e).code();

            throw new IgniteSQLException("Failed to parse DDL statement: " + sql + ": " + e.getMessage(),
                code, e);
        }
    }

    /**
     * Parse and split query if needed, cache either two-step query or statement.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param batched Batched flag.
     * @param remainingAllowed Whether multiple statements are allowed.
     * @return Parsing result.
     */
    @SuppressWarnings("IfMayBeConditional")
    private QueryParserResult parseH2(String schemaName, SqlFieldsQuery qry, boolean batched,
        boolean remainingAllowed) {
        try (H2PooledConnection c = connMgr.connection(schemaName)) {
            // For queries that are explicitly local, we rely on the flag specified in the query
            // because this parsing result will be cached and used for queries directly.
            // For other queries, we enforce join order at this stage to avoid premature optimizations
            // (and therefore longer parsing) as long as there'll be more parsing at split stage.
            boolean enforceJoinOrderOnParsing = (!qry.isLocal() || qry.isEnforceJoinOrder());

            QueryContext qctx = QueryContext.parseContext(idx.backupFilter(null, null), qry.isLocal());

            H2Utils.setupConnection(
                c,
                qctx,
                false,
                enforceJoinOrderOnParsing,
                false);

            PreparedStatement stmt = null;

            try {
                stmt = c.prepareStatementNoCache(qry.getSql());

                if (qry.isLocal() && GridSqlQueryParser.checkMultipleStatements(stmt))
                    throw new IgniteSQLException("Multiple statements queries are not supported for local queries.",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                GridSqlQueryParser.PreparedWithRemaining prep = GridSqlQueryParser.preparedWithRemaining(stmt);

                Prepared prepared = prep.prepared();

                if (GridSqlQueryParser.isExplainUpdate(prepared))
                    throw new IgniteSQLException("Explains of update queries are not supported.",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                // Get remaining query and check if it is allowed.
                SqlFieldsQuery remainingQry = null;

                if (!F.isEmpty(prep.remainingSql())) {
                    checkRemainingAllowed(remainingAllowed);

                    remainingQry = cloneFieldsQuery(qry).setSql(prep.remainingSql());
                }

                // Prepare new query.
                SqlFieldsQuery newQry = cloneFieldsQuery(qry).setSql(prepared.getSQL());

                final int paramsCnt = prepared.getParameters().size();

                Object[] argsOrig = qry.getArgs();

                Object[] args = null;
                Object[] remainingArgs = null;

                if (!batched && paramsCnt > 0) {
                    if (argsOrig == null || argsOrig.length < paramsCnt)
                        // Not enough parameters, but we will handle this later on execution phase.
                        args = argsOrig;
                    else {
                        args = Arrays.copyOfRange(argsOrig, 0, paramsCnt);

                        if (paramsCnt != argsOrig.length)
                            remainingArgs = Arrays.copyOfRange(argsOrig, paramsCnt, argsOrig.length);
                    }
                }
                else
                    remainingArgs = argsOrig;

                newQry.setArgs(args);

                QueryDescriptor newQryDesc = queryDescriptor(schemaName, newQry);

                if (remainingQry != null)
                    remainingQry.setArgs(remainingArgs);

                final List<JdbcParameterMeta> paramsMeta;

                try {
                    paramsMeta = H2Utils.parametersMeta(stmt.getParameterMetaData());

                    assert prepared.getParameters().size() == paramsMeta.size();
                }
                catch (IgniteCheckedException | SQLException e) {
                    throw new IgniteSQLException("Failed to get parameters metadata", IgniteQueryErrorCode.UNKNOWN, e);
                }

                // Do actual parsing.
                if (CommandProcessor.isCommand(prepared)) {
                    GridSqlStatement cmdH2 = new GridSqlQueryParser(false, log).parse(prepared);

                    QueryParserResultCommand cmd = new QueryParserResultCommand(null, cmdH2, false);

                    return new QueryParserResult(
                        newQryDesc,
                        queryParameters(newQry),
                        remainingQry,
                        paramsMeta,
                        null,
                        null,
                        cmd
                    );
                }
                else if (CommandProcessor.isCommandNoOp(prepared)) {
                    QueryParserResultCommand cmd = new QueryParserResultCommand(null, null, true);

                    return new QueryParserResult(
                        newQryDesc,
                        queryParameters(newQry),
                        remainingQry,
                        paramsMeta,
                        null,
                        null,
                        cmd
                    );
                }
                else if (GridSqlQueryParser.isDml(prepared)) {
                    QueryParserResultDml dml = prepareDmlStatement(newQryDesc, prepared);

                    return new QueryParserResult(
                        newQryDesc,
                        queryParameters(newQry),
                        remainingQry,
                        paramsMeta,
                        null,
                        dml,
                        null
                    );
                }
                else if (!prepared.isQuery()) {
                    throw new IgniteSQLException("Unsupported statement: " + newQry.getSql(),
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }

                // Parse SELECT.
                GridSqlQueryParser parser = new GridSqlQueryParser(false, log);

                GridSqlQuery selectStmt = (GridSqlQuery)parser.parse(prepared);

                List<Integer> cacheIds = parser.cacheIds();
                Integer mvccCacheId = mvccCacheIdForSelect(parser.objectsMap());

                // Calculate if query is in fact can be executed locally.
                boolean loc = qry.isLocal();

                if (!loc) {
                    if (parser.isLocalQuery())
                        loc = true;
                }

                // If this is a local query, check if it must be split.
                boolean locSplit = false;

                if (loc) {
                    GridCacheContext cctx = parser.getFirstPartitionedCache();

                    if (cctx != null && cctx.config().getQueryParallelism() > 1)
                        locSplit = true;
                }

                // Split is required either if query is distributed, or when it is local, but executed
                // over segmented PARTITIONED case. In this case multiple map queries will be executed against local
                // node stripes in parallel and then merged through reduce process.
                boolean splitNeeded = !loc || locSplit;

                String forUpdateQryOutTx = null;
                String forUpdateQryTx = null;
                GridCacheTwoStepQuery forUpdateTwoStepQry = null;

                boolean forUpdate = GridSqlQueryParser.isForUpdateQuery(prepared);

                // SELECT FOR UPDATE case handling. We need to create extra queries with appended _key
                // column to be able to lock selected rows further.
                if (forUpdate) {
                    // We have checked above that it's not an UNION query, so it's got to be SELECT.
                    assert selectStmt instanceof GridSqlSelect;

                    // Check FOR UPDATE invariants: only one table, MVCC is there.
                    if (cacheIds.size() != 1)
                        throw new IgniteSQLException("SELECT FOR UPDATE is supported only for queries " +
                            "that involve single transactional cache.");

                    if (mvccCacheId == null)
                        throw new IgniteSQLException("SELECT FOR UPDATE query requires transactional cache " +
                            "with MVCC enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                    // We need a copy because we are going to modify AST a bit. We do not want to modify original select.
                    GridSqlSelect selForUpdate = ((GridSqlSelect)selectStmt).copySelectForUpdate();

                    // Clear forUpdate flag to run it as a plain query.
                    selForUpdate.forUpdate(false);
                    ((GridSqlSelect)selectStmt).forUpdate(false);

                    // Remember sql string without FOR UPDATE clause.
                    forUpdateQryOutTx = selForUpdate.getSQL();

                    GridSqlAlias keyCol = keyColumn(selForUpdate);

                    selForUpdate.addColumn(keyCol, true);

                    // Remember sql string without FOR UPDATE clause and with _key column.
                    forUpdateQryTx = selForUpdate.getSQL();

                    // Prepare additional two-step query for FOR UPDATE case.
                    if (splitNeeded) {
                        c.schema(newQry.getSchema());

                        forUpdateTwoStepQry = GridSqlQuerySplitter.split(
                            c,
                            selForUpdate,
                            forUpdateQryTx,
                            newQry.isCollocated(),
                            newQry.isDistributedJoins(),
                            newQry.isEnforceJoinOrder(),
                            locSplit,
                            idx,
                            paramsCnt,
                            log
                        );
                    }
                }

                GridCacheTwoStepQuery twoStepQry = null;

                if (splitNeeded) {
                    c.schema(newQry.getSchema());

                    twoStepQry = GridSqlQuerySplitter.split(
                        c,
                        selectStmt,
                        newQry.getSql(),
                        newQry.isCollocated(),
                        newQry.isDistributedJoins(),
                        newQry.isEnforceJoinOrder(),
                        locSplit,
                        idx,
                        paramsCnt,
                        log
                    );
                }

                List<GridQueryFieldMetadata> meta = H2Utils.meta(stmt.getMetaData());

                QueryParserResultSelect select = new QueryParserResultSelect(
                    selectStmt,
                    twoStepQry,
                    forUpdateTwoStepQry,
                    meta,
                    cacheIds,
                    mvccCacheId,
                    forUpdateQryOutTx,
                    forUpdateQryTx
                );

                return new QueryParserResult(
                    newQryDesc,
                    queryParameters(newQry),
                    remainingQry,
                    paramsMeta,
                    select,
                    null,
                    null
                );
            }
            catch (IgniteCheckedException | SQLException e) {
                throw new IgniteSQLException("Failed to parse query. " + e.getMessage(), IgniteQueryErrorCode.PARSING, e);
            }
            finally {
                U.close(stmt, log);
            }
        }
    }

    /**
     * Throw exception is multiple statements are not allowed.
     *
     * @param allowed Whether multiple statements are allowed.
     */
    private static void checkRemainingAllowed(boolean allowed) {
        if (allowed)
            return;

        throw new IgniteSQLException("Multiple statements queries are not supported.",
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Get ID of the first MVCC cache for SELECT.
     *
     * @param objMap Object map.
     * @return ID of the first MVCC cache or {@code null} if no MVCC caches involved.
     */
    private Integer mvccCacheIdForSelect(Map<Object, Object> objMap) {
        Boolean mvccEnabled = null;
        Integer mvccCacheId = null;
        GridCacheContextInfo cctx = null;

        for (Object o : objMap.values()) {
            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst)o);
            if (o instanceof GridSqlTable && ((GridSqlTable)o).dataTable() != null) {
                GridSqlTable tbl = (GridSqlTable)o;

                if (tbl.dataTable() != null) {
                    GridCacheContextInfo curCctx = tbl.dataTable().cacheInfo();

                    assert curCctx != null;

                    boolean curMvccEnabled =
                        curCctx.config().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

                    if (mvccEnabled == null) {
                        mvccEnabled = curMvccEnabled;

                        if (mvccEnabled)
                            mvccCacheId = curCctx.cacheId();

                        cctx = curCctx;
                    }
                    else if (mvccEnabled != curMvccEnabled)
                        MvccUtils.throwAtomicityModesMismatchException(cctx.config(), curCctx.config());
                }
            }
        }

        return mvccCacheId;
    }

    /**
     * Prepare DML statement.
     *
     * @param planKey Plan key.
     * @param prepared Prepared.
     * @return Statement.
     */
    private QueryParserResultDml prepareDmlStatement(QueryDescriptor planKey, Prepared prepared) {
        if (F.eq(QueryUtils.SCHEMA_SYS, planKey.schemaName()))
            throw new IgniteSQLException("DML statements are not supported on " + planKey.schemaName() + " schema",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        // Prepare AST.
        GridSqlQueryParser parser = new GridSqlQueryParser(false, log);

        GridSqlStatement stmt = parser.parse(prepared);

        List<GridH2Table> tbls = parser.tablesForDml();

        // Check if caches are started because we may need to collect affinity info later on, so they needs to be
        // available on local node.
        for (GridH2Table h2tbl : tbls)
            H2Utils.checkAndStartNotStartedCache(idx.kernalContext(), h2tbl);

        // Check MVCC mode.
        GridCacheContextInfo ctx = null;
        boolean mvccEnabled = false;

        for (GridH2Table h2tbl : tbls) {
            GridCacheContextInfo curCtx = h2tbl.cacheInfo();
            boolean curMvccEnabled = curCtx.config().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

            if (ctx == null) {
                ctx = curCtx;

                mvccEnabled = curMvccEnabled;
            }
            else if (curMvccEnabled != mvccEnabled)
                MvccUtils.throwAtomicityModesMismatchException(ctx.config(), curCtx.config());
        }

        // Get streamer info.
        GridH2Table streamTbl = null;

        if (GridSqlQueryParser.isStreamableInsertStatement(prepared)) {
            GridSqlInsert insert = (GridSqlInsert)stmt;

            streamTbl = DmlAstUtils.gridTableForElement(insert.into()).dataTable();
        }

        // Create update plan.
        UpdatePlan plan;

        try {
            plan = UpdatePlanBuilder.planForStatement(
                planKey,
                stmt,
                mvccEnabled,
                idx,
                log
            );
        }
        catch (Exception e) {
            if (e instanceof IgniteSQLException)
                throw (IgniteSQLException)e;
            else
                throw new IgniteSQLException("Failed to prepare update plan.", e);
        }

        return new QueryParserResultDml(
            stmt,
            mvccEnabled,
            streamTbl,
            plan
        );
    }

    /**
     * Clear cached plans.
     */
    public void clearCache() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);
    }

    /**
     * Check expected statement type (when it is set by JDBC) and given statement type.
     *
     * @param qry Query.
     * @param isQry {@code true} for select queries, otherwise (DML/DDL queries) {@code false}.
     */
    private static void checkQueryType(SqlFieldsQuery qry, boolean isQry) {
        Boolean qryFlag = qry instanceof SqlFieldsQueryEx ? ((SqlFieldsQueryEx) qry).isQuery() : null;

        if (qryFlag != null && qryFlag != isQry)
            throw new IgniteSQLException("Given statement type does not match that declared by JDBC driver",
                IgniteQueryErrorCode.STMT_TYPE_MISMATCH);
    }

    /**
     * Make a copy of {@link SqlFieldsQuery} with all flags and preserving type.
     *
     * @param oldQry Query to copy.
     * @return Query copy.
     */
    private static SqlFieldsQuery cloneFieldsQuery(SqlFieldsQuery oldQry) {
        return oldQry.copy().setLocal(oldQry.isLocal()).setPageSize(oldQry.getPageSize());
    }

    /**
     * Prepare plan key.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @return Plan key.
     */
    private static QueryDescriptor queryDescriptor(String schemaName, SqlFieldsQuery qry) {
        boolean skipReducerOnUpdate = false;
        boolean batched = false;

        if (qry instanceof SqlFieldsQueryEx) {
            SqlFieldsQueryEx qry0 = (SqlFieldsQueryEx)qry;

            skipReducerOnUpdate = !qry.isLocal() && qry0.isSkipReducerOnUpdate();
            batched = qry0.isBatched();
        }

        return new QueryDescriptor(
            schemaName,
            qry.getSql(),
            qry.isCollocated(),
            qry.isDistributedJoins(),
            qry.isEnforceJoinOrder(),
            qry.isLocal(),
            skipReducerOnUpdate,
            batched
        );
    }
}
