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

package org.apache.ignite.internal.sql;

import java.lang.reflect.Field;
import java.util.HashSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * SQL keyword constants.
 */
public class SqlKeyword {
    /** Keyword: KILL. */
    public static final String KILL = "KILL";

    /** Keyword: QUERY. */
    public static final String QUERY = "QUERY";

    /** Keyword: SCAN. */
    public static final String SCAN = "SCAN";

    /** Keyword: CONTINUOUS. */
    public static final String CONTINUOUS = "CONTINUOUS";

    /** Keyword: COMPUTE_TASK. */
    public static final String COMPUTE = "COMPUTE";

    /** Keyword: SERVICE. */
    public static final String SERVICE = "SERVICE";

    /** Keyword: ALTER. */
    public static final String ALTER = "ALTER";

    /** Keyword: ALLOW_OVERWRITE */
    public static final String ALLOW_OVERWRITE = "ALLOW_OVERWRITE";

    /** Keyword: ASC. */
    public static final String ASC = "ASC";

    /** Keyword: BATCH_SIZE */
    public static final String BATCH_SIZE = "BATCH_SIZE";

    /** Keyword: BEGIN. */
    public static final String BEGIN = "BEGIN";

    /** Keyword: BIGINT */
    public static final String BIGINT = "BIGINT";

    /** Keyword: BIT. */
    public static final String BIT = "BIT";

    /** Keyword: BOOL. */
    public static final String BOOL = "BOOL";

    /** Keyword: PACKET_SIZE. */
    public static final String PACKET_SIZE = "PACKET_SIZE";

    /** Keyword: BOOLEAN. */
    public static final String BOOLEAN = "BOOLEAN";

    /** Keyword: CASCADE. */
    public static final String CASCADE = "CASCADE";

    /** Keyword: CHAR. */
    public static final String CHAR = "CHAR";

    /** Keyword: CHARACTER. */
    public static final String CHARACTER = "CHARACTER";

    /** Keyword: CHARSET. */
    public static final String CHARSET = "CHARSET";

    /** Keyword: COMMIT. */
    public static final String COMMIT = "COMMIT";

    /** Keyword: COPY. */
    public static final String COPY = "COPY";

    /** Keyword: CREATE. */
    public static final String CREATE = "CREATE";

    /** Keyword: DATE. */
    public static final String DATE = "DATE";

    /** Keyword: DATETIME. */
    public static final String DATETIME = "DATETIME";

    /** Keyword: DEC. */
    public static final String DEC = "DEC";

    /** Keyword: DECIMAL. */
    public static final String DECIMAL = "DECIMAL";

    /** Keyword: DESC. */
    public static final String DESC = "DESC";

    /** Keyword: DOUBLE. */
    public static final String DOUBLE = "DOUBLE";

    /** Keyword: DROP. */
    public static final String DROP = "DROP";

    /** Keyword: EXISTS. */
    public static final String EXISTS = "EXISTS";

    /** Keyword: FLOAT. */
    public static final String FLOAT = "FLOAT";

    /** Keyword: FLOAT4. */
    public static final String FLOAT4 = "FLOAT4";

    /** Keyword: FLOAT8. */
    public static final String FLOAT8 = "FLOAT8";

    /** Keyword: FLUSH_FREQUENCY. */
    public static final String FLUSH_FREQUENCY = "FLUSH_FREQUENCY";

    /** Keyword: FORMAT. */
    public static final String FORMAT = "FORMAT";

    /** Keyword: FROM. */
    public static final String FROM = "FROM";

    /** Keyword: FULLTEXT. */
    public static final String FULLTEXT = "FULLTEXT";

    /** Keyword: UNIQUE. */
    public static final String HASH = "HASH";

    /** Keyword: IF. */
    public static final String IF = "IF";

    /** Keyword: INDEX. */
    public static final String INDEX = "INDEX";

    /** Keyword: INLINE_SIZE. */
    public static final String INLINE_SIZE = "INLINE_SIZE";

    /** Keyword: INT. */
    public static final String INT = "INT";

    /** Keyword: INT2. */
    public static final String INT2 = "INT2";

    /** Keyword: INT4. */
    public static final String INT4 = "INT4";

    /** Keyword: INT8. */
    public static final String INT8 = "INT8";

    /** Keyword: INTEGER. */
    public static final String INTEGER = "INTEGER";

    /** Keyword: INTO. */
    public static final String INTO = "INTO";

    /** Keyword: KEY. */
    public static final String KEY = "KEY";

    /** Keyword: LOGGING. */
    public static final String LOGGING = "LOGGING";

    /** Keyword: LONGVARCHAR. */
    public static final String LONGVARCHAR = "LONGVARCHAR";

    /** Keyword: MEDIUMINT. */
    public static final String MEDIUMINT = "MEDIUMINT";

    /** Keyword: NCHAR. */
    public static final String NCHAR = "NCHAR";

    /** Keyword: NOLOGGING. */
    public static final String NOLOGGING = "NOLOGGING";

    /** Keyword: NOT. */
    public static final String NOT = "NOT";

    /** Keyword: NUMBER. */
    public static final String NUMBER = "NUMBER";

    /** Keyword: NUMERIC. */
    public static final String NUMERIC = "NUMERIC";

    /** Keyword: NVARCHAR. */
    public static final String NVARCHAR = "NVARCHAR";

    /** Keyword: NVARCHAR2. */
    public static final String NVARCHAR2 = "NVARCHAR2";

    /** Keyword: OFF. */
    public static final String OFF = "OFF";

    /** Keyword: ON. */
    public static final String ON = "ON";

    /** Keyword: ORDERED. */
    public static final String ORDERED = "ORDERED";

    /** Keyword: PER_NODE_PARALLEL_OPERATIONS. */
    public static final String PER_NODE_PARALLEL_OPERATIONS = "PER_NODE_PARALLEL_OPERATIONS";

    /** Keyword: PER_NODE_BUFFER_SIZE. */
    public static final String PER_NODE_BUFFER_SIZE = "PER_NODE_BUFFER_SIZE";

    /** Keyword: PRECISION. */
    public static final String PRECISION = "PRECISION";

    /** Keyword: PRIMARY. */
    public static final String PRIMARY = "PRIMARY";

    /** Keyword: REAL. */
    public static final String REAL = "REAL";

    /** Keyword: RESTRICT. */
    public static final String RESTRICT = "RESTRICT";

    /** Keyword: ROLLBACK. */
    public static final String ROLLBACK = "ROLLBACK";

    /** Keyword: SET. */
    public static final String SET = "SET";

    /** Keyword: SIGNED. */
    public static final String SIGNED = "SIGNED";

    /** Keyword: SMALLDATETIME. */
    public static final String SMALLDATETIME = "SMALLDATETIME";

    /** Keyword: SMALLINT. */
    public static final String SMALLINT = "SMALLINT";

    /** Keyword: SPATIAL. */
    public static final String SPATIAL = "SPATIAL";

    /** Keyword: START. */
    public static final String START = "START";

    /** Keyword: STREAMING. */
    public static final String STREAMING = "STREAMING";

    /** Keyword: TABLE. */
    public static final String TABLE = "TABLE";

    /** Keyword: TIME. */
    public static final String TIME = "TIME";

    /** Keyword: TIMESTAMP. */
    public static final String TIMESTAMP = "TIMESTAMP";

    /** Keyword: TINYINT. */
    public static final String TINYINT = "TINYINT";

    /** Keyword: TRANSACTION. */
    public static final String TRANSACTION = "TRANSACTION";

    /** Keyword: UNIQUE. */
    public static final String UNIQUE = "UNIQUE";

    /** Keyword: UUID. */
    public static final String UUID = "UUID";

    /** Keyword: VARCHAR. */
    public static final String VARCHAR = "VARCHAR";

    /** Keyword: VARCHAR2. */
    public static final String VARCHAR2 = "VARCHAR2";

    /** Keyword: VARCHAR_CASESENSITIVE. */
    public static final String VARCHAR_CASESENSITIVE = "VARCHAR_CASESENSITIVE";

    /** Keyword: WORK. */
    public static final String WORK = "WORK";

    /** Keyword: YEAR. */
    public static final String YEAR = "YEAR";

    /** Keyword: PARALLEL. */
    public static final String PARALLEL = "PARALLEL";

    /** Keyword: USER. */
    public static final String USER = "USER";

    /** Keyword: WITH. */
    public static final String WITH = "WITH";

    /** Keyword: PASSWORD. */
    public static final String PASSWORD = "PASSWORD";

    /** Keyword: SHOW. */
    public static final String SHOW = "SHOW";

    /** Keyword: HELP. */
    public static final String HELP = "HELP";

    /** Keyword: GRANT. */
    public static final String GRANT = "GRANT";

    /** Keyword: REVOKE. */
    public static final String REVOKE = "REVOKE";

    /** All keywords. */
    private static final HashSet<String> KEYWORDS;

    static {
        KEYWORDS = new HashSet<>();

        try {
            for (Field field : SqlKeyword.class.getDeclaredFields()) {
                if (F.eq(String.class, field.getType())) {
                    String val = (String) field.get(null);

                    KEYWORDS.add(val);
                }
            }
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to initialize keywords collection.", e);
        }
    }

    /**
     * Check if string is a keyword.
     *
     * @param str String.
     * @return {@code True} if it is a keyword.
     */
    public static boolean isKeyword(String str) {
        return KEYWORDS.contains(str);
    }

    /**
     * Private constructor.
     */
    private SqlKeyword() {
        // No-op.
    }
}
