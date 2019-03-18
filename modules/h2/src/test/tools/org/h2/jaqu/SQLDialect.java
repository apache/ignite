/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import org.h2.jaqu.TableDefinition.IndexDefinition;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * This interface defines points where JaQu can build different statements
 * depending on the database used.
 */
public interface SQLDialect {

    /**
     * Get the SQL snippet for the table name.
     *
     * @param schema the schema name, or null for no schema
     * @param table the table name
     * @return the SQL snippet
     */
    String getTableName(String schema, String table);

    /**
     * Get the CREATE INDEX statement.
     *
     * @param schema the schema name
     * @param table the table name
     * @param index the index definition
     * @return the SQL statement
     */
    String getCreateIndex(String schema, String table, IndexDefinition index);

    /**
     * Append "LIMIT limit" to the SQL statement.
     *
     * @param stat the statement
     * @param limit the limit
     */
    void appendLimit(SQLStatement stat, long limit);

    /**
     * Append "OFFSET offset" to the SQL statement.
     *
     * @param stat the statement
     * @param offset the offset
     */
    void appendOffset(SQLStatement stat, long offset);

    /**
     * Whether memory tables are supported.
     *
     * @return true if they are
     */
    boolean supportsMemoryTables();

    /**
     * Default implementation of an SQL dialect. Designed for an H2 database,
     * and may be suitable for others.
     */
    public static class DefaultSQLDialect implements SQLDialect {

        @Override
        public String getTableName(String schema, String table) {
            if (StringUtils.isNullOrEmpty(schema)) {
                return table;
            }
            return schema + "." + table;
        }

        @Override
        public boolean supportsMemoryTables() {
            return true;
        }

        @Override
        public String getCreateIndex(String schema, String table,
                IndexDefinition index) {
            StatementBuilder buff = new StatementBuilder();
            buff.append("CREATE ");
            switch (index.type) {
            case STANDARD:
                break;
            case UNIQUE:
                buff.append("UNIQUE ");
                break;
            case HASH:
                buff.append("HASH ");
                break;
            case UNIQUE_HASH:
                buff.append("UNIQUE HASH ");
                break;
            }
            buff.append("INDEX IF NOT EXISTS ");
            buff.append(index.indexName);
            buff.append(" ON ");
            buff.append(table);
            buff.append("(");
            for (String col : index.columnNames) {
                buff.appendExceptFirst(", ");
                buff.append(col);
            }
            buff.append(")");
            return buff.toString();
        }

        @Override
        public void appendLimit(SQLStatement stat, long limit) {
            stat.appendSQL(" LIMIT " + limit);
        }

        @Override
        public void appendOffset(SQLStatement stat, long offset) {
            stat.appendSQL(" OFFSET " + offset);
        }

    }

}
