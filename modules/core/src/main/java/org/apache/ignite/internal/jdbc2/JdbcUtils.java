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

package org.apache.ignite.internal.jdbc2;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcColumnMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcIndexMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcTableMeta;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;

import static java.sql.DatabaseMetaData.columnNullable;
import static java.sql.DatabaseMetaData.tableIndexOther;
import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

/**
 * Utility methods for JDBC driver.
 */
public class JdbcUtils {
    /** The only possible name for catalog. */
    public static final String CATALOG_NAME = "IGNITE";

    /** Name of TABLE type. */
    public static final String TYPE_TABLE = "TABLE";

    /** Name of VIEW type. */
    public static final String TYPE_VIEW = "VIEW";

    /**
     * Converts Java class name to type from {@link Types}.
     *
     * @param cls Java class name.
     * @return Type from {@link Types}.
     */
    public static int type(String cls) {
        if (Boolean.class.getName().equals(cls) || boolean.class.getName().equals(cls))
            return BOOLEAN;
        else if (Byte.class.getName().equals(cls) || byte.class.getName().equals(cls))
            return TINYINT;
        else if (Short.class.getName().equals(cls) || short.class.getName().equals(cls))
            return SMALLINT;
        else if (Integer.class.getName().equals(cls) || int.class.getName().equals(cls))
            return INTEGER;
        else if (Long.class.getName().equals(cls) || long.class.getName().equals(cls))
            return BIGINT;
        else if (Float.class.getName().equals(cls) || float.class.getName().equals(cls))
            return FLOAT;
        else if (Double.class.getName().equals(cls) || double.class.getName().equals(cls))
            return DOUBLE;
        else if (String.class.getName().equals(cls))
            return VARCHAR;
        else if (byte[].class.getName().equals(cls))
            return BINARY;
        else if (Time.class.getName().equals(cls))
            return TIME;
        else if (Timestamp.class.getName().equals(cls))
            return TIMESTAMP;
        else if (Date.class.getName().equals(cls) || java.sql.Date.class.getName().equals(cls))
            return DATE;
        else if (BigDecimal.class.getName().equals(cls))
            return DECIMAL;
        else
            return OTHER;
    }

    /**
     * Converts Java class name to SQL type name.
     *
     * @param cls Java class name.
     * @return SQL type name.
     */
    public static String typeName(String cls) {
        if (Boolean.class.getName().equals(cls) || boolean.class.getName().equals(cls))
            return "BOOLEAN";
        else if (Byte.class.getName().equals(cls) || byte.class.getName().equals(cls))
            return "TINYINT";
        else if (Short.class.getName().equals(cls) || short.class.getName().equals(cls))
            return "SMALLINT";
        else if (Integer.class.getName().equals(cls) || int.class.getName().equals(cls))
            return "INTEGER";
        else if (Long.class.getName().equals(cls) || long.class.getName().equals(cls))
            return "BIGINT";
        else if (Float.class.getName().equals(cls) || float.class.getName().equals(cls))
            return "FLOAT";
        else if (Double.class.getName().equals(cls) || double.class.getName().equals(cls))
            return "DOUBLE";
        else if (String.class.getName().equals(cls))
            return "VARCHAR";
        else if (byte[].class.getName().equals(cls))
            return "BINARY";
        else if (Time.class.getName().equals(cls))
            return "TIME";
        else if (Timestamp.class.getName().equals(cls))
            return "TIMESTAMP";
        else if (Date.class.getName().equals(cls) || java.sql.Date.class.getName().equals(cls))
            return "DATE";
        else if (BigDecimal.class.getName().equals(cls))
            return "DECIMAL";
        else
            return "OTHER";
    }

    /**
     * Determines whether type is nullable.
     *
     * @param name Column name.
     * @param cls Java class name.
     * @return {@code True} if nullable.
     */
    public static boolean nullable(String name, String cls) {
        return !"_KEY".equalsIgnoreCase(name) &&
            !"_VAL".equalsIgnoreCase(name) &&
            !(boolean.class.getName().equals(cls) ||
            byte.class.getName().equals(cls) ||
            short.class.getName().equals(cls) ||
            int.class.getName().equals(cls) ||
            long.class.getName().equals(cls) ||
            float.class.getName().equals(cls) ||
            double.class.getName().equals(cls));
    }

    /**
     * Checks whether a class is SQL-compliant.
     *
     * @param cls Class.
     * @return Whether given type is SQL-compliant.
     */
    static boolean isSqlType(Class<?> cls) {
        return QueryUtils.isSqlType(cls) || cls == URL.class;
    }

    /**
     * Convert exception to {@link SQLException}.
     *
     * @param e Converted Exception.
     * @param msgForUnknown Message non-convertable exception.
     * @return JDBC {@link SQLException}.
     * @see IgniteQueryErrorCode
     */
    public static SQLException convertToSqlException(Exception e, String msgForUnknown) {
        return convertToSqlException(e, msgForUnknown, null);
    }

    /**
     * Convert exception to {@link SQLException}.
     *
     * @param e Converted Exception.
     * @param msgForUnknown Message for non-convertable exception.
     * @param sqlStateForUnknown SQLSTATE for non-convertable exception.
     * @return JDBC {@link SQLException}.
     * @see IgniteQueryErrorCode
     */
    public static SQLException convertToSqlException(Exception e, String msgForUnknown, String sqlStateForUnknown) {
        SQLException sqlEx = null;

        Throwable t = e;

        while (sqlEx == null && t != null) {
            if (t instanceof SQLException)
                return (SQLException)t;
            else if (t instanceof IgniteSQLException)
                return ((IgniteSQLException)t).toJdbcException();

            t = t.getCause();
        }

        return new SQLException(msgForUnknown, sqlStateForUnknown, e);
    }

    /**
     * @param colMeta Column metadata.
     * @param pos Ordinal position.
     * @return Column metadata row.
     */
    public static List<Object> columnRow(JdbcColumnMeta colMeta, int pos) {
        List<Object> row = new ArrayList<>(24);

        row.add(CATALOG_NAME);                  // 1. TABLE_CAT
        row.add(colMeta.schemaName());          // 2. TABLE_SCHEM
        row.add(colMeta.tableName());           // 3. TABLE_NAME
        row.add(colMeta.columnName());          // 4. COLUMN_NAME
        row.add(colMeta.dataType());            // 5. DATA_TYPE
        row.add(colMeta.dataTypeName());        // 6. TYPE_NAME
        row.add(colMeta.precision() == -1 ? null : colMeta.precision()); // 7. COLUMN_SIZE
        row.add((Integer)null);                 // 8. BUFFER_LENGTH
        row.add(colMeta.scale() == -1 ? null : colMeta.scale());           // 9. DECIMAL_DIGITS
        row.add(10);                            // 10. NUM_PREC_RADIX
        row.add(colMeta.isNullable() ? columnNullable : columnNoNulls);  // 11. NULLABLE
        row.add((String)null);                  // 12. REMARKS
        row.add(colMeta.defaultValue());        // 13. COLUMN_DEF
        row.add(colMeta.dataType());            // 14. SQL_DATA_TYPE
        row.add((Integer)null);                 // 15. SQL_DATETIME_SUB
        row.add(Integer.MAX_VALUE);             // 16. CHAR_OCTET_LENGTH
        row.add(pos);                           // 17. ORDINAL_POSITION
        row.add(colMeta.isNullable() ? "YES" : "NO"); // 18. IS_NULLABLE
        row.add((String)null);                  // 19. SCOPE_CATALOG
        row.add((String)null);                  // 20. SCOPE_SCHEMA
        row.add((String)null);                  // 21. SCOPE_TABLE
        row.add((Short)null);                   // 22. SOURCE_DATA_TYPE
        row.add("NO");                          // 23. IS_AUTOINCREMENT
        row.add("NO");                          // 23. IS_GENERATEDCOLUMN

        return row;
    }

    /**
     * @param idxMeta Index metadata.
     * @return List of result rows correspond to index.
     */
    public static List<List<Object>> indexRows(JdbcIndexMeta idxMeta) {
        List<List<Object>> rows = new ArrayList<>(idxMeta.fields().size());

        for (int i = 0; i < idxMeta.fields().size(); ++i) {
            List<Object> row = new ArrayList<>(13);

            row.add(CATALOG_NAME);              // TABLE_CAT
            row.add(idxMeta.schemaName());      // TABLE_SCHEM
            row.add(idxMeta.tableName());       // TABLE_NAME
            row.add(true);                      // NON_UNIQUE
            row.add(null);                      // INDEX_QUALIFIER (index catalog)
            row.add(idxMeta.indexName());       // INDEX_NAME
            row.add(tableIndexOther);           // TYPE
            row.add(i + 1);                     // ORDINAL_POSITION
            row.add(idxMeta.fields().get(i));   // COLUMN_NAME
            row.add(idxMeta.fieldsAsc().get(i) ? "A" : "D");  // ASC_OR_DESC
            row.add((Integer)0);                // CARDINALITY
            row.add((Integer)0);                // PAGES
            row.add((String)null);              // FILTER_CONDITION

            rows.add(row);
        }

        return rows;
    }

    /**
     * @param pkMeta Primary key metadata.
     * @return Result set rows for primary key.
     */
    public static List<List<Object>> primaryKeyRows(JdbcPrimaryKeyMeta pkMeta) {
        List<List<Object>> rows = new ArrayList<>(pkMeta.fields().size());

        for (int i = 0; i < pkMeta.fields().size(); ++i) {
            List<Object> row = new ArrayList<>(6);

            row.add(CATALOG_NAME); // table catalog
            row.add(pkMeta.schemaName());
            row.add(pkMeta.tableName());
            row.add(pkMeta.fields().get(i));
            row.add(i + 1); // sequence number
            row.add(pkMeta.name());

            rows.add(row);
        }

        return rows;
    }

    /**
     * @param tblMeta Table metadata.
     * @return Table metadata row.
     */
    public static List<Object> tableRow(JdbcTableMeta tblMeta) {
        List<Object> row = new ArrayList<>(10);

        row.add(CATALOG_NAME);
        row.add(tblMeta.schemaName());
        row.add(tblMeta.tableName());
        row.add(tblMeta.tableType());
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);

        return row;
    }

    /**
     * Normalize schema name. If it is quoted - unquote and leave as is, otherwise - convert to upper case.
     *
     * @param schemaName Schema name.
     * @return Normalized schema name.
     */
    public static String normalizeSchema(String schemaName) {
        if (F.isEmpty(schemaName))
            return QueryUtils.DFLT_SCHEMA;

        String res;

        if (schemaName.startsWith("\"") && schemaName.endsWith("\""))
            res = schemaName.substring(1, schemaName.length() - 1);
        else
            res = schemaName.toUpperCase();

        return res;
    }
}
