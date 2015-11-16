package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC query execute request.
 */
public class QueryExecuteRequest extends GridOdbcRequest {
    /** Sql query. */
    private String sqlQry;

    /** Sql query arguments. */
    private Object[] args;

    /** Cache name. */
    private String cacheName;

    /**
     * @param cacheName Cache name.
     * @param sqlQry SQL query.
     */
    public QueryExecuteRequest(String cacheName, String sqlQry) {
        this(cacheName, sqlQry, null);
    }

    /**
     * @param cacheName Cache name.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public QueryExecuteRequest(String cacheName, String sqlQry, Object[] args) {
        super(EXECUTE_SQL_QUERY);
        this.cacheName = cacheName;
        this.sqlQry = sqlQry;
        this.args = args;
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
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }
}
