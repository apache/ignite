package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC command request.
 */
public class GridOdbcRequest {
    /** Execute sql query. */
    public static final int EXECUTE_SQL_QUERY = 1;

    /** Fetch query results. */
    public static final int FETCH_SQL_QUERY = 2;

    /** Close query. */
    public static final int CLOSE_SQL_QUERY = 3;

    /** Command. */
    private int cmd;

    /**
     * @param cmd Command type.
     */
    public GridOdbcRequest(int cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public int command() {
        return cmd;
    }

    /**
     * @param cmd Command.
     */
    public void command(int cmd) {
        this.cmd = cmd;
    }
}
