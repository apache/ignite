/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;
import java.util.List;
import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.table.TableView;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

/**
 * A prepared statement.
 */
public abstract class Prepared {

    /**
     * The session.
     */
    protected Session session;

    /**
     * The SQL string.
     */
    protected String sqlStatement;

    /**
     * Whether to create a new object (for indexes).
     */
    protected boolean create = true;

    /**
     * The list of parameters.
     */
    protected ArrayList<Parameter> parameters;

    /**
     * If the query should be prepared before each execution. This is set for
     * queries with LIKE ?, because the query plan depends on the parameter
     * value.
     */
    protected boolean prepareAlways;

    private long modificationMetaId;
    private Command command;
    private int objectId;
    private int currentRowNumber;
    private int rowScanCount;
    /**
     * Common table expressions (CTE) in queries require us to create temporary views,
     * which need to be cleaned up once a command is done executing.
     */
    private List<TableView> cteCleanups;

    /**
     * Create a new object.
     *
     * @param session the session
     */
    public Prepared(Session session) {
        this.session = session;
        modificationMetaId = session.getDatabase().getModificationMetaId();
    }

    /**
     * Check if this command is transactional.
     * If it is not, then it forces the current transaction to commit.
     *
     * @return true if it is
     */
    public abstract boolean isTransactional();

    /**
     * Get an empty result set containing the meta data.
     *
     * @return the result set
     */
    public abstract ResultInterface queryMeta();


    /**
     * Get the command type as defined in CommandInterface
     *
     * @return the statement type
     */
    public abstract int getType();

    /**
     * Check if this command is read only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return false;
    }

    /**
     * Check if the statement needs to be re-compiled.
     *
     * @return true if it must
     */
    public boolean needRecompile() {
        Database db = session.getDatabase();
        if (db == null) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "database closed");
        }
        // parser: currently, compiling every create/drop/... twice
        // because needRecompile return true even for the first execution
        return prepareAlways ||
                modificationMetaId < db.getModificationMetaId() ||
                db.getSettings().recompileAlways;
    }

    /**
     * Get the meta data modification id of the database when this statement was
     * compiled.
     *
     * @return the meta data modification id
     */
    long getModificationMetaId() {
        return modificationMetaId;
    }

    /**
     * Set the meta data modification id of this statement.
     *
     * @param id the new id
     */
    void setModificationMetaId(long id) {
        this.modificationMetaId = id;
    }

    /**
     * Set the parameter list of this statement.
     *
     * @param parameters the parameter list
     */
    public void setParameterList(ArrayList<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * Get the parameter list.
     *
     * @return the parameter list
     */
    public ArrayList<Parameter> getParameters() {
        return parameters;
    }

    /**
     * Check if all parameters have been set.
     *
     * @throws DbException if any parameter has not been set
     */
    protected void checkParameters() {
        if (parameters != null) {
            for (Parameter param : parameters) {
                param.checkSet();
            }
        }
    }

    /**
     * Set the command.
     *
     * @param command the new command
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * Check if this object is a query.
     *
     * @return true if it is
     */
    public boolean isQuery() {
        return false;
    }

    /**
     * Prepare this statement.
     */
    public void prepare() {
        // nothing to do
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    public int update() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the query.
     *
     * @param maxrows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    @SuppressWarnings("unused")
    public ResultInterface query(int maxrows) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Set the SQL statement.
     *
     * @param sql the SQL statement
     */
    public void setSQL(String sql) {
        this.sqlStatement = sql;
    }

    /**
     * Get the SQL statement.
     *
     * @return the SQL statement
     */
    public String getSQL() {
        return sqlStatement;
    }

    /**
     * Get the object id to use for the database object that is created in this
     * statement. This id is only set when the object is persistent.
     * If not set, this method returns 0.
     *
     * @return the object id or 0 if not set
     */
    protected int getCurrentObjectId() {
        return objectId;
    }

    /**
     * Get the current object id, or get a new id from the database. The object
     * id is used when creating new database object (CREATE statement).
     *
     * @return the object id
     */
    protected int getObjectId() {
        int id = objectId;
        if (id == 0) {
            id = session.getDatabase().allocateObjectId();
        } else {
            objectId = 0;
        }
        return id;
    }

    /**
     * Get the SQL statement with the execution plan.
     *
     * @return the execution plan
     */
    public String getPlanSQL() {
        return null;
    }

    /**
     * Check if this statement was canceled.
     *
     * @throws DbException if it was canceled
     */
    public void checkCanceled() {
        session.checkCanceled();
        Command c = command != null ? command : session.getCurrentCommand();
        if (c != null) {
            c.checkCanceled();
        }
    }

    /**
     * Set the object id for this statement.
     *
     * @param i the object id
     */
    public void setObjectId(int i) {
        this.objectId = i;
        this.create = false;
    }

    /**
     * Set the session for this statement.
     *
     * @param currentSession the new session
     */
    public void setSession(Session currentSession) {
        this.session = currentSession;
    }

    /**
     * Print information about the statement executed if info trace level is
     * enabled.
     *
     * @param startTimeNanos when the statement was started
     * @param rowCount the query or update row count
     */
    void trace(long startTimeNanos, int rowCount) {
        if (session.getTrace().isInfoEnabled() && startTimeNanos > 0) {
            long deltaTimeNanos = System.nanoTime() - startTimeNanos;
            String params = Trace.formatParams(parameters);
            session.getTrace().infoSQL(sqlStatement, params, rowCount,
                    deltaTimeNanos / 1000 / 1000);
        }
        // startTime_nanos can be zero for the command that actually turns on
        // statistics
        if (session.getDatabase().getQueryStatistics() && startTimeNanos != 0) {
            long deltaTimeNanos = System.nanoTime() - startTimeNanos;
            session.getDatabase().getQueryStatisticsData().
                    update(toString(), deltaTimeNanos, rowCount);
        }
    }

    /**
     * Set the prepare always flag.
     * If set, the statement is re-compiled whenever it is executed.
     *
     * @param prepareAlways the new value
     */
    public void setPrepareAlways(boolean prepareAlways) {
        this.prepareAlways = prepareAlways;
    }

    /**
     * Set the current row number.
     *
     * @param rowNumber the row number
     */
    protected void setCurrentRowNumber(int rowNumber) {
        if ((++rowScanCount & 127) == 0) {
            checkCanceled();
        }
        this.currentRowNumber = rowNumber;
        setProgress();
    }

    /**
     * Get the current row number.
     *
     * @return the row number
     */
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    /**
     * Notifies query progress via the DatabaseEventListener
     */
    private void setProgress() {
        if ((currentRowNumber & 127) == 0) {
            session.getDatabase().setProgress(
                    DatabaseEventListener.STATE_STATEMENT_PROGRESS,
                    sqlStatement, currentRowNumber, 0);
        }
    }

    /**
     * Convert the statement to a String.
     *
     * @return the SQL statement
     */
    @Override
    public String toString() {
        return sqlStatement;
    }

    /**
     * Get the SQL snippet of the value list.
     *
     * @param values the value list
     * @return the SQL snippet
     */
    protected static String getSQL(Value[] values) {
        StatementBuilder buff = new StatementBuilder();
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            if (v != null) {
                buff.append(v.getSQL());
            }
        }
        return buff.toString();
    }

    /**
     * Get the SQL snippet of the expression list.
     *
     * @param list the expression list
     * @return the SQL snippet
     */
    protected static String getSQL(Expression[] list) {
        StatementBuilder buff = new StatementBuilder();
        for (Expression e : list) {
            buff.appendExceptFirst(", ");
            if (e != null) {
                buff.append(e.getSQL());
            }
        }
        return buff.toString();
    }

    /**
     * Set the SQL statement of the exception to the given row.
     *
     * @param e the exception
     * @param rowId the row number
     * @param values the values of the row
     * @return the exception
     */
    protected DbException setRow(DbException e, int rowId, String values) {
        StringBuilder buff = new StringBuilder();
        if (sqlStatement != null) {
            buff.append(sqlStatement);
        }
        buff.append(" -- ");
        if (rowId > 0) {
            buff.append("row #").append(rowId + 1).append(' ');
        }
        buff.append('(').append(values).append(')');
        return e.addSQL(buff.toString());
    }

    public boolean isCacheable() {
        return false;
    }

    /**
     * @return the temporary views created for CTE's.
     */
    public List<TableView> getCteCleanups() {
        return cteCleanups;
    }

    /**
     * Set the temporary views created for CTE's.
     *
     * @param cteCleanups the temporary views
     */
    public void setCteCleanups(List<TableView> cteCleanups) {
        this.cteCleanups = cteCleanups;
    }

    public Session getSession() {
        return session;
    }
}
