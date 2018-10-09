package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.command.Prepared;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;

/**
 * Holder for all changing set of parameters passed for executing queries SQL fields
 */
public class TaskSqlFields {
    /** */
    private String schemaName;

    /** */
    private SqlFieldsQuery qry;

    /** */
    private SqlClientContext cliCtx;

    /** */
    private boolean keepBinary;

    /** */
    private boolean failOnMultipleStmts;

    /** */
    private MvccQueryTracker tracker;

    /** */
    private GridQueryCancel cancel;

    /** */
    private boolean startTx;

    /** */
    private List<GridQueryFieldMetadata> meta;

    /** */
    private Prepared preparedCmd;

    /** */
    private GridCacheTwoStepQuery twoStepQry;

    /** */
    private IndexingQueryFilter filter;

    /** */
    private int[] cacheIds = new int[0];

    /** */
    private String qrySubmitted;

    /** */
    private GridCacheQueryType qryTypeSubmitted;

    /**
     *
     * @return New instance.
     */
    public static TaskSqlFields create() {
        return new TaskSqlFields();
    }

    /** */
    private TaskSqlFields() {
    }

    /**
     *
     * @param schemaName Schema name.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addSchemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     *
     * @param qry Query.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addSqlFieldsQuery(SqlFieldsQuery qry) {
        this.qry = qry;

        return this;
    }

    /**
     *
     * @param cliCtx Client context.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addSqlClientContext(SqlClientContext cliCtx) {
        this.cliCtx = cliCtx;

        return this;
    }

    /**
     *
     * @param keepBinary If to keep binary.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addKeepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;

        return this;
    }

    /**
     *
     * @param failOnMultipleStmts If to fail on multiple statements.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addFailOnMultipleStmts(boolean failOnMultipleStmts) {
        this.failOnMultipleStmts = failOnMultipleStmts;

        return this;
    }

    /**
     *
     * @param tracker Mvcc query tracker.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addMvccQueryTracker(MvccQueryTracker tracker) {
        this.tracker = tracker;

        return this;
    }

    /**
     *
     * @param cancel Query cancel.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addGridQueryCancel(GridQueryCancel cancel) {
        this.cancel = cancel;

        return this;
    }

    /**
     *
     * @param startTx If to start transaction.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addStartTx(boolean startTx) {
        this.startTx = startTx;

        return this;
    }

    /**
     *
     * @param meta List of mete data.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addMeta(List<GridQueryFieldMetadata> meta) {
        this.meta = meta;

        return this;
    }

    /**
     *
     * @param preparedCmd Prepared command.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addPreparedCmd(Prepared preparedCmd) {
        this.preparedCmd = preparedCmd;

        return this;
    }

    /**
     *
     * @param twoStepQry Two step query.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addTwoStepQry(GridCacheTwoStepQuery twoStepQry) {
        this.twoStepQry = twoStepQry;

        return this;
    }

    public TaskSqlFields addFilter(IndexingQueryFilter filter) {
        this.filter = filter;

        return this;
    }

    /**
     *
     * @param cacheIds Array of cache ids.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addCacheIds(int[] cacheIds) {
        this.cacheIds = cacheIds;

        return this;
    }

    /**
     *
     * @param qrySubmitted Text of query that was actually submitted.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addQuerySubmitted(String qrySubmitted) {
        this.qrySubmitted = qrySubmitted;

        return this;
    }

    /**
     *
     * @param qryTypeSubmitted Text of query that was actually submitted.
     * @return This instance for chain calls.
     */
    public TaskSqlFields addQueryTypeSubmitted(GridCacheQueryType qryTypeSubmitted) {
        this.qryTypeSubmitted = qryTypeSubmitted;

        return this;
    }

    /** */
    public String schemaName() {
        return schemaName;
    }

    /** */
    public SqlFieldsQuery qry() {
        return qry;
    }

    /** */
    public SqlClientContext cliCtx() {
        return cliCtx;
    }

    /** */
    public boolean keepBinary() {
        return keepBinary;
    }

    /** */
    public boolean failOnMultipleStmts() {
        return failOnMultipleStmts;
    }

    /** */
    public MvccQueryTracker tracker() {
        return tracker;
    }

    /** */
    public GridQueryCancel cancel() {
        return cancel;
    }

    /** */
    public boolean startTx() {
        return startTx;
    }

    /** */
    public List<GridQueryFieldMetadata> meta() {
        return meta;
    }

    /** */
    public Prepared preparedCommand() {
        return preparedCmd;
    }

    /** */
    public GridCacheTwoStepQuery twoStepQuery() {
        return twoStepQry;
    }

    /** */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /** */
    public int[] cacheIds() {
        return cacheIds;
    }

    /** */
    public String querySubmitted() {
        return qrySubmitted;
    }

    /** */
    public GridCacheQueryType queryTypeSubmitted() {
        return qryTypeSubmitted;
    }
}
