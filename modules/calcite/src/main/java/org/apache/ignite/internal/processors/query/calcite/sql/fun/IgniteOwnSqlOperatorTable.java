/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Operator table that contains Ignite own functions and operators.
 *
 * Implementors for operators should be added to
 * {@link org.apache.ignite.internal.processors.query.calcite.exec.exp.RexImpTable}
 */
public class IgniteOwnSqlOperatorTable extends ReflectiveSqlOperatorTable {
    /**
     * The table of contains Ignite-specific operators.
     */
    private static IgniteOwnSqlOperatorTable instance;

    /**
     *
     */
    public static final SqlFunction LENGTH =
        new SqlFunction(
            "LENGTH",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.CHARACTER,
            SqlFunctionCategory.NUMERIC);

    /**
     *
     */
    public static final SqlFunction SYSTEM_RANGE = new SqlSystemRangeFunction();

    /**
     *
     */
    public static final SqlFunction TYPEOF =
        new SqlFunction(
            "TYPEOF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM);

    /**
     * Query engine name.
     */
    public static final SqlFunction QUERY_ENGINE =
        new SqlFunction(
            "QUERY_ENGINE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.NILADIC,
            SqlFunctionCategory.SYSTEM);

    /**
     * Replacement for NULL values in search bounds. Required to distinguish searchable NULL values
     * (for example, 'a IS NULL' condition) and not searchable NULL values (for example, 'a = NULL' condition).
     *
     * Note: System function, cannot be used by user.
     */
    public static final SqlFunction NULL_BOUND =
        new SqlFunction(
            "$NULL_BOUND",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.ANY),
            null,
            OperandTypes.NILADIC,
            SqlFunctionCategory.SYSTEM);

    /**
     * Least of two arguments. Unlike LEAST, which is converted to CASE WHEN THEN END clause, this function
     * is natively implemented.
     *
     * Note: System function, cannot be used by user.
     */
    public static final SqlFunction LEAST2 =
        new SqlFunction(
            "$LEAST2",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
            null,
            OperandTypes.SAME_SAME,
            SqlFunctionCategory.SYSTEM);

    /**
     * Greatest of two arguments. Unlike GREATEST, which is converted to CASE WHEN THEN END clause, this function
     * is natively implemented.
     *
     * Note: System function, cannot be used by user.
     */
    public static final SqlFunction GREATEST2 =
        new SqlFunction(
            "$GREATEST2",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
            null,
            OperandTypes.SAME_SAME,
            SqlFunctionCategory.SYSTEM);

    /** Bitwise '&' of two values. */
    public static final SqlFunction BITAND =
        new SqlFunction(
            "BITAND",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.LEAST_RESTRICTIVE,
            InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
            SqlFunctionCategory.NUMERIC);

    /** Bitwise '|' of two values. */
    public static final SqlFunction BITOR =
        new SqlFunction(
            "BITOR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.LEAST_RESTRICTIVE,
            InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
            SqlFunctionCategory.NUMERIC);

    /** Bitwise '^' of two values. */
    public static final SqlFunction BITXOR =
        new SqlFunction(
            "BITXOR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.LEAST_RESTRICTIVE,
            InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
            SqlFunctionCategory.NUMERIC);

    /**
     * Returns the Ignite operator table, creating it if necessary.
     */
    public static synchronized IgniteOwnSqlOperatorTable instance() {
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the
            // table until the constructor of the sub-class has completed.
            instance = new IgniteOwnSqlOperatorTable();
            instance.init();
        }
        return instance;
    }
}
