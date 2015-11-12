package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.processors.odbc.protocol.GridOdbcCommand;

/**
 * ODBC command request.
 */
public class GridOdbcRequest {

    /** Command. */
    private GridOdbcCommand cmd;

    /** Sql query. */
    private String sqlQry;

    /** Sql query arguments. */
    private Object[] args;

    /** Page size. */
    private Integer pageSize;

    /**
     * @param sqlQry SQL query.
     * @param pageSize Page size.
     */
    public GridOdbcRequest(String sqlQry, int pageSize) {
        this(sqlQry, pageSize, null);
    }

    /**
     * @param sqlQry SQL query.
     * @param pageSize Page size.
     * @param args Arguments list.
     */
    public GridOdbcRequest(String sqlQry, int pageSize, Object[] args) {
        this.cmd = GridOdbcCommand.EXECUTE_SQL_QUERY;
        this.sqlQry = sqlQry;
        this.pageSize = pageSize;
        this.args = args;
    }

    /**
     * @return Command.
     */
    public GridOdbcCommand command() {
        return cmd;
    }

    /**
     * @param cmd Command.
     */
    public void command(GridOdbcCommand cmd) {
        this.cmd = cmd;
    }

    /**
     * @param sqlQry Sql query.
     */
    public void sqlQuery(String sqlQry) {
        this.sqlQry = sqlQry;
    }

    /**
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /**
     * @param args Sql query arguments.
     */
    public void arguments(Object[] args) {
        this.args = args;
    }

    /**
     * @return Sql query arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @param pageSize Page size.
     */
    public void pageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

}
