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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUtils;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
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
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Parser module. Splits incoming request into a series of parsed results.
 */
public class QueryParser {
    /** */
    private static final int CACHE_SIZE = 1024;

    /** A pattern for commands having internal implementation in Ignite. */
    public static final Pattern INTERNAL_CMD_RE = Pattern.compile(
        "^(create|drop)\\s+index|^alter\\s+table|^copy|^set|^begin|^commit|^rollback|^(create|alter|drop)\\s+user",
        Pattern.CASE_INSENSITIVE);

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private volatile GridBoundedConcurrentLinkedHashMap<QueryParserCacheKey, QueryParserCacheEntry> cache =
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

        log = idx.kernalContext().log(QueryParser.class);
    }

    /**
     * Parse the query.
     *
     * @param schemaName schema name.
     * @param qry query to parse.
     * @return Parsing result that contains Parsed leading query and remaining sql script.
     */
    public QueryParserResult parse(String schemaName, SqlFieldsQuery qry) {
        QueryParserResult res = parse0(schemaName, qry);

        checkQueryType(qry, res.isSelect());

        return res;
    }

    /**
     * Parse the query.
     *
     * @param schemaName schema name.
     * @param qry query to parse.
     * @return Parsing result that contains Parsed leading query and remaining sql script.
     */
    private QueryParserResult parse0(String schemaName, SqlFieldsQuery qry) {
        // First, let's check if we already have a two-step query for this statement...
        QueryParserCacheKey cachedKey = new QueryParserCacheKey(
            schemaName,
            qry.getSql(),
            qry.isCollocated(),
            qry.isDistributedJoins(),
            qry.isEnforceJoinOrder(),
            qry.isLocal()
        );

        QueryParserCacheEntry cached = cache.get(cachedKey);

        if (cached != null)
            return new QueryParserResult(qry, null, cached.select(), cached.dml(), cached.command());

        // Try parting as native command.
        QueryParserResult parseRes = parseNative(schemaName, qry);

        // Otherwise parse with H2.
        if (parseRes == null)
            parseRes = parseH2(schemaName, qry);

        // Add to cache if not multi-statement.
        if (parseRes.remainingQuery() == null) {
            cached = new QueryParserCacheEntry(parseRes.select(), parseRes.dml(), parseRes.command());

            cache.put(cachedKey, cached);
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
     * @return Command or {@code null} if cannot parse this query.
     */
    @SuppressWarnings("IfMayBeConditional")
    @Nullable
    private QueryParserResult parseNative(String schemaName, SqlFieldsQuery qry) {
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
                || nativeCmd instanceof SqlDropUserCommand)
            )
                return null;

            SqlFieldsQuery newQry = cloneFieldsQuery(qry).setSql(parser.lastCommandSql());

            SqlFieldsQuery remainingQry;

            if (F.isEmpty(parser.remainingSql()))
                remainingQry = null;
            else
                remainingQry = cloneFieldsQuery(qry).setSql(parser.remainingSql()).setArgs(qry.getArgs());

            QueryParserResultCommand cmd = new QueryParserResultCommand(nativeCmd, null, false);

            return new QueryParserResult(newQry, remainingQry, null, null, cmd);
        }
        catch (SqlStrictParseException e) {
            throw new IgniteSQLException(e.getMessage(), IgniteQueryErrorCode.PARSING, e);
        }
        catch (Exception e) {
            // Cannot parse, return.
            if (log.isDebugEnabled())
                log.debug("Failed to parse SQL with native parser [qry=" + sql + ", err=" + e + ']');

            if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK))
                return null;

            int code = IgniteQueryErrorCode.PARSING;

            if (e instanceof SqlParseException)                code = ((SqlParseException)e).code();

            throw new IgniteSQLException("Failed to parse DDL statement: " + sql + ": " + e.getMessage(),
                code, e);
        }
    }

    /**
     * Parse and split query if needed, cache either two-step query or statement.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @return Parsing result.
     */
    @SuppressWarnings("IfMayBeConditional")
    private QueryParserResult parseH2(String schemaName, SqlFieldsQuery qry) {
        Connection c = connMgr.connectionForThread().connection(schemaName);

        // For queries that are explicitly local, we rely on the flag specified in the query
        // because this parsing result will be cached and used for queries directly.
        // For other queries, we enforce join order at this stage to avoid premature optimizations
        // (and therefore longer parsing) as long as there'll be more parsing at split stage.
        boolean enforceJoinOrderOnParsing = (!qry.isLocal() || qry.isEnforceJoinOrder());

        H2Utils.setupConnection(c, /*distributedJoins*/false, /*enforceJoinOrder*/enforceJoinOrderOnParsing);

        PreparedStatement stmt;

        try {
            stmt = connMgr.prepareStatement(c, qry.getSql());
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to parse query. " + e.getMessage(),
                IgniteQueryErrorCode.PARSING, e);
        }

        if (qry.isLocal() && GridSqlQueryParser.checkMultipleStatements(stmt))
            throw new IgniteSQLException("Multiple statements queries are not supported for local queries.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        GridSqlQueryParser.PreparedWithRemaining prep = GridSqlQueryParser.preparedWithRemaining(stmt);

        Prepared prepared = prep.prepared();

        if (GridSqlQueryParser.isExplainUpdate(prepared))
            throw new IgniteSQLException("Explains of update queries are not supported.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        int paramsCnt = prepared.getParameters().size();

        Object[] argsOrig = qry.getArgs();

        Object[] args = null;
        Object[] remainingArgs = null;

        if (!DmlUtils.isBatched(qry) && paramsCnt > 0) {
            if (argsOrig == null || argsOrig.length < paramsCnt) {
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + (argsOrig != null ? argsOrig.length + 1 : 1) + " parameter.");
            }

            args = Arrays.copyOfRange(argsOrig, 0, paramsCnt);

            if (paramsCnt != argsOrig.length)
                remainingArgs = Arrays.copyOfRange(argsOrig, paramsCnt, argsOrig.length);
        }
        else
            remainingArgs = argsOrig;

        SqlFieldsQuery remainingQry;

        if (F.isEmpty(prep.remainingSql()))
            remainingQry = null;
        else
            remainingQry = cloneFieldsQuery(qry).setSql(prep.remainingSql()).setArgs(remainingArgs);

        SqlFieldsQuery newQry = cloneFieldsQuery(qry).setSql(prepared.getSQL()).setArgs(args);

        if (CommandProcessor.isCommand(prepared)) {
            GridSqlStatement cmdH2 = new GridSqlQueryParser(false).parse(prepared);

            QueryParserResultCommand cmd = new QueryParserResultCommand(null, cmdH2, false);

            return new QueryParserResult(newQry, remainingQry, null, null, cmd);
        }
        else if (CommandProcessor.isCommandNoOp(prepared)) {
            QueryParserResultCommand cmd = new QueryParserResultCommand(null, null, true);

            return new QueryParserResult(newQry, remainingQry, null, null, cmd);
        }
        else if (GridSqlQueryParser.isDml(prepared))
            return new QueryParserResult(newQry, remainingQry, null ,new QueryParserResultDml(prepared), null);
        else if (!prepared.isQuery()) {
            throw new IgniteSQLException("Unsupported statement: " + newQry.getSql(),
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        // Parse SELECT. Split is required either if query is distirubted, or when it is local, but executed
        // over segmented PARTITIONED case. In this case multiple map queries will be executed against local
        // node stripes in parallel and then merged through reduce process.


        // Calculate if query is in fact can be executed locally.
        boolean loc = qry.isLocal();

        GridSqlQueryParser parser = null;

        if (!loc) {
            parser = new GridSqlQueryParser(false);

            parser.parse(prepared);

            if (parser.isLocalQuery())
                loc = true;
        }

        // If this is a local query, check if it must be split.
        boolean locSplit = false;

        if (loc) {
            if (parser == null) {
                parser = new GridSqlQueryParser(false);

                parser.parse(prepared);
            }

            GridCacheContext cctx = parser.getFirstPartitionedCache();

            if (cctx != null && cctx.config().getQueryParallelism() > 1)
                locSplit = true;
        }

        boolean splitNeeded = !loc || locSplit;

        try {
            GridCacheTwoStepQuery twoStepQry = null;

            if (splitNeeded) {
                twoStepQry = GridSqlQuerySplitter.split(
                    connMgr.connectionForThread().connection(newQry.getSchema()),
                    prepared,
                    newQry.getArgs(),
                    newQry.isCollocated(),
                    newQry.isDistributedJoins(),
                    newQry.isEnforceJoinOrder(),
                    locSplit,
                    idx
                );
            }

            List<GridQueryFieldMetadata> meta = H2Utils.meta(stmt.getMetaData());

            QueryParserResultSelect select = new QueryParserResultSelect(twoStepQry, locSplit, meta);

            return new QueryParserResult(newQry, remainingQry, select, null, null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to parse query: " + newQry.getSql(), IgniteQueryErrorCode.PARSING, e);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
        finally {
            // TODO: Leak if returned earlier. Will be fixed in https://issues.apache.org/jira/browse/IGNITE-11279
            U.close(stmt, log);
        }
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
}
