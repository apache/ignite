package org.apache.ignite.console.agent.utils;

import org.apache.ignite.console.agent.db.*;
import org.apache.ignite.console.agent.db.introspector.DatabaseIntrospector;
import org.apache.ignite.console.agent.db.introspector.OracleIntrospector;
import org.apache.ignite.console.agent.db.introspector.PGIntrospector;
import org.apache.ignite.console.agent.db.introspector.SqlServerIntrospector;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 数据库操作
 *
 * @author liuzh
 */
public class DBMetadataUtils {   
    private Connection connection;
    private LetterCase letterCase;
    private Dialect dialect;
    private DatabaseIntrospector introspector;    
    private List<String> catalogs;
    private List<String> schemas;

    public DBMetadataUtils(Connection connection,Dialect dialect) {
        this(connection, dialect, false, true);
    }

    public DBMetadataUtils(Connection connection, Dialect dialect, boolean forceBigDecimals, boolean useCamelCase) {
        if (connection == null) {
            throw new NullPointerException("Argument connection can't be null!");
        }
        this.connection = connection;
        this.dialect = dialect;
        try {
            initLetterCase();
            this.introspector = getDatabaseIntrospector(forceBigDecimals, useCamelCase);            
            this.catalogs = introspector.getCatalogs();
            this.schemas = introspector.getSchemas();
            
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initLetterCase() {
        try {
            DatabaseMetaData databaseMetaData = getConnection().getMetaData();
            if (databaseMetaData.storesLowerCaseIdentifiers()) {
                letterCase = LetterCase.LOWER;
            } else if (databaseMetaData.storesUpperCaseIdentifiers()) {
                letterCase = LetterCase.UPPER;
            } else {
                letterCase = LetterCase.NORMAL;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String convertLetterByCase(String value) {
        if (value == null) {
            return null;
        }
        switch (letterCase) {
            case UPPER:
                return value.toUpperCase();
            case LOWER:
                return value.toLowerCase();
            default:
                return value;
        }
    }

    public DatabaseMetaData getDatabaseMetaData() throws SQLException {
    	return getConnection().getMetaData();
    }

    public DatabaseIntrospector getIntrospector() {
        return introspector;
    }

    private DatabaseIntrospector getDatabaseIntrospector(boolean forceBigDecimals, boolean useCamelCase) {
        switch (dialect) {
            case ORACLE:
                return new OracleIntrospector(this, forceBigDecimals, useCamelCase);
            case POSTGRESQL:
                return new PGIntrospector(this, forceBigDecimals, useCamelCase);
            case SQLSERVER:
                return new SqlServerIntrospector(this, forceBigDecimals, useCamelCase);
            case DB2:
            case HSQLDB:
            case MARIADB:
            case MYSQL:
            default:
                return new DatabaseIntrospector(this, forceBigDecimals, useCamelCase);
        }
    }

    public List<String> getCatalogs() throws SQLException {
        return catalogs;
    }

    public List<String> getSchemas() throws SQLException {
        return schemas;
    }

    public List<IntrospectedTable> introspectTables(DatabaseConfig config) throws SQLException {
    	return introspector.introspectTables(config);
    }

    public static void sortTables(List<IntrospectedTable> tables) {
        if (SqlStringUtils.isNotEmpty(tables)) {
            Collections.sort(tables, new Comparator<IntrospectedTable>() {
                public int compare(IntrospectedTable o1, IntrospectedTable o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
        }
    }

    public static void sortColumns(List<IntrospectedColumn> columns) {
        if (SqlStringUtils.isNotEmpty(columns)) {
            Collections.sort(columns, new Comparator<IntrospectedColumn>() {
                public int compare(IntrospectedColumn o1, IntrospectedColumn o2) {
                    int result = o1.getTableName().compareTo(o2.getTableName());
                    if (result == 0) {
                        result = o1.getName().compareTo(o2.getName());
                    }
                    return result;
                }
            });
        }
    }

   

    public boolean testConnection() {
        try {
            if (!getConnection().isClosed()) {
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public Connection getConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            return connection;
        }
        throw new SQLException("connection is closed!");
    }

    private enum LetterCase {
        UPPER, LOWER, NORMAL
    }

    public Dialect getDialect() {
        return dialect;
    }

    /**
     * 获取默认的Config,任何情况下都会返回一个
     *
     * @return
     * @throws SQLException
     */
    public DatabaseConfig getDefaultConfig() throws SQLException {
        DatabaseConfig config = null;
        if (catalogs.size() == 1) {
            if (schemas.size() == 1) {
                config = new DatabaseConfig(catalogs.get(0), schemas.get(0));
            } else if (schemas.size() == 0) {
                config = new DatabaseConfig(catalogs.get(0), null);
            }
        } else if (catalogs.size() == 0) {
            if (schemas.size() == 1) {
                config = new DatabaseConfig(null, schemas.get(0));
            } else if (schemas.size() == 0) {
                config = new DatabaseConfig(null, null);
            }
        }
        if (config == null) {
            String dbName = connection.getCatalog();
            switch (getDialect()) {
                case DB2:
                case ORACLE:
                    config = new DatabaseConfig(null, connection.getClientInfo("user"));
                    break;
                case MYSQL:
                    if (schemas.size() > 0) {
                        break;
                    }
                    if (dbName != null) {
                        for (String catalog : catalogs) {
                            if (dbName.equalsIgnoreCase(catalog)) {
                                config = new DatabaseConfig(catalog, null);
                                break;
                            }
                        }
                    }
                    break;
                case SQLSERVER:
                    String sqlserverCatalog = null;
                    if (dbName != null) {
                        for (String catalog : catalogs) {
                            if (dbName.equalsIgnoreCase(catalog)) {
                                sqlserverCatalog = catalog;
                                break;
                            }
                        }
                        if (sqlserverCatalog != null) {
                            for (String schema : schemas) {
                                if ("dbo".equalsIgnoreCase(schema)) {
                                    config = new DatabaseConfig(sqlserverCatalog, "dbo");
                                    break;
                                }
                            }
                        }
                    }
                    break;
                case POSTGRESQL:
                    String postgreCatalog = null;
                    if (dbName != null) {
                        for (String catalog : catalogs) {
                            if (dbName.equalsIgnoreCase(catalog)) {
                                postgreCatalog = catalog;
                                break;
                            }
                        }
                        if (postgreCatalog != null) {
                            for (String schema : schemas) {
                                if ("public".equalsIgnoreCase(schema)) {
                                    config = new DatabaseConfig(postgreCatalog, "public");
                                    break;
                                }
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        if(config == null){
            config = new DatabaseConfig(null, null);
        }
        return config;
    }

}
