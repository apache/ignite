package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC query get columns meta request.
 */
public class QueryGetColumnsMetaRequest extends GridOdbcRequest {
    /** Cache name. */
    private String cacheName;

    /** Table name. */
    private String tableName;

    /** Column name. */
    private String columnName;

    /**
     * @param cacheName Cache name.
     */
    public QueryGetColumnsMetaRequest(String cacheName, String tableName, String columnName) {
        super(GET_COLUMNS_META);

        this.cacheName = cacheName;
        this.tableName = tableName;
        this.columnName = columnName;
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

    /**
     * @param tableName Table name.
     */
    public void tableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @param columnName Column name.
     */
    public void columnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return columnName;
    }
}