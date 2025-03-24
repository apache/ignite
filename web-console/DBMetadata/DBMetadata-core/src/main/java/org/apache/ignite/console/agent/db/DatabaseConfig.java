package org.apache.ignite.console.agent.db;

/**
 * 数据库查询配置
 *
 * @author liuzh
 */
public class DatabaseConfig {
    private String catalog;
    private String schemaPattern;
    private String tableNamePattern;
    private DatabaseProcess databaseProcess;

    public DatabaseConfig() {
        this(null, null);
    }

    public DatabaseConfig(String catalog, String schemaPattern) {
        this(catalog, schemaPattern, "%");
    }

    public DatabaseConfig(String catalog, String schemaPattern, String tableNamePattern) {
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.tableNamePattern = tableNamePattern;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchemaPattern() {
        return schemaPattern;
    }

    public void setSchemaPattern(String schemaPattern) {
        this.schemaPattern = schemaPattern;
    }

    public String getTableNamePattern() {
        return tableNamePattern;
    }

    public void setTableNamePattern(String tableNamePattern) {
        this.tableNamePattern = tableNamePattern;
    }

    public boolean hasProcess() {
        return databaseProcess != null;
    }

    public DatabaseProcess getDatabaseProcess() {
        return databaseProcess;
    }

    public void setDatabaseProcess(DatabaseProcess databaseProcess) {
        this.databaseProcess = databaseProcess;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatabaseConfig that = (DatabaseConfig) o;

        if (catalog != null ? !catalog.equals(that.catalog) : that.catalog != null) return false;
        if (schemaPattern != null ? !schemaPattern.equals(that.schemaPattern) : that.schemaPattern != null)
            return false;
        if (tableNamePattern != null ? !tableNamePattern.equals(that.tableNamePattern) : that.tableNamePattern != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = catalog != null ? catalog.hashCode() : 0;
        result = 31 * result + (schemaPattern != null ? schemaPattern.hashCode() : 0);
        result = 31 * result + (tableNamePattern != null ? tableNamePattern.hashCode() : 0);
        return result;
    }
}
