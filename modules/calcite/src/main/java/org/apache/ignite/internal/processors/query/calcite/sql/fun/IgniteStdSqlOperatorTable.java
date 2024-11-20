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

import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexImpTable;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteConvertletTable;

/**
 * Operator table that contains subset of Calcite's library operators supported by Ignite.
 *
 * @see RexImpTable for functions and operators implementors.
 * @see IgniteConvertletTable for functions and operators convertlets.
 * @see Accumulators for aggregates implementors.
 *
 * Note: Actially we don't use reflective capabilities of ReflectiveSqlOperatorTable (init method never called), but
 * this class have infrastructure to register operators manualy and for fast operators lookup, so it's handy to use it.
 */
public class IgniteStdSqlOperatorTable extends ReflectiveSqlOperatorTable {
    /** Singleton instance. */
    public static final IgniteStdSqlOperatorTable INSTANCE = new IgniteStdSqlOperatorTable();

    /**
     * Default constructor.
     */
    public IgniteStdSqlOperatorTable() {
        // Set operators.
        register(SqlStdOperatorTable.UNION);
        register(SqlStdOperatorTable.UNION_ALL);
        register(SqlStdOperatorTable.EXCEPT);
        register(SqlStdOperatorTable.EXCEPT_ALL);
        register(SqlStdOperatorTable.INTERSECT);
        register(SqlStdOperatorTable.INTERSECT_ALL);

        // Logical.
        register(SqlStdOperatorTable.AND);
        register(SqlStdOperatorTable.OR);
        register(SqlStdOperatorTable.NOT);

        // Comparisons.
        register(SqlStdOperatorTable.LESS_THAN);
        register(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        register(SqlStdOperatorTable.GREATER_THAN);
        register(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        register(SqlStdOperatorTable.EQUALS);
        register(SqlStdOperatorTable.NOT_EQUALS);
        register(SqlStdOperatorTable.BETWEEN);
        register(SqlStdOperatorTable.NOT_BETWEEN);

        // Arithmetic.
        register(SqlStdOperatorTable.PLUS);
        register(SqlStdOperatorTable.MINUS);
        register(SqlStdOperatorTable.MULTIPLY);
        register(SqlStdOperatorTable.DIVIDE);
        register(SqlStdOperatorTable.DIVIDE_INTEGER); // Used internally.
        register(SqlStdOperatorTable.PERCENT_REMAINDER);
        register(SqlStdOperatorTable.UNARY_MINUS);
        register(SqlStdOperatorTable.UNARY_PLUS);

        // Aggregates.
        register(SqlStdOperatorTable.COUNT);
        register(SqlStdOperatorTable.SUM);
        register(SqlStdOperatorTable.SUM0);
        register(SqlStdOperatorTable.AVG);
        register(SqlStdOperatorTable.MIN);
        register(SqlStdOperatorTable.MAX);
        register(SqlStdOperatorTable.ANY_VALUE);
        register(SqlStdOperatorTable.SINGLE_VALUE);
        register(SqlStdOperatorTable.FILTER);
        register(SqlLibraryOperators.GROUP_CONCAT);
        register(SqlLibraryOperators.STRING_AGG);
        register(SqlStdOperatorTable.LISTAGG);
        register(SqlLibraryOperators.ARRAY_AGG);
        register(SqlLibraryOperators.ARRAY_CONCAT_AGG);
        register(SqlStdOperatorTable.EVERY);
        register(SqlStdOperatorTable.SOME);

        // IS ... operator.
        register(SqlStdOperatorTable.IS_NULL);
        register(SqlStdOperatorTable.IS_NOT_NULL);
        register(SqlStdOperatorTable.IS_TRUE);
        register(SqlStdOperatorTable.IS_NOT_TRUE);
        register(SqlStdOperatorTable.IS_FALSE);
        register(SqlStdOperatorTable.IS_NOT_FALSE);
        register(SqlStdOperatorTable.IS_DISTINCT_FROM);
        register(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);

        // LIKE and SIMILAR.
        register(SqlStdOperatorTable.LIKE);
        register(SqlStdOperatorTable.NOT_LIKE);
        register(SqlStdOperatorTable.SIMILAR_TO);
        register(SqlStdOperatorTable.NOT_SIMILAR_TO);

        // NULLS ordering.
        register(SqlStdOperatorTable.NULLS_FIRST);
        register(SqlStdOperatorTable.NULLS_LAST);
        register(SqlStdOperatorTable.DESC);

        // Exists.
        register(SqlStdOperatorTable.EXISTS);

        // String functions.
        register(SqlStdOperatorTable.UPPER);
        register(SqlStdOperatorTable.LOWER);
        register(SqlStdOperatorTable.INITCAP);
        register(SqlLibraryOperators.TO_BASE64);
        register(SqlLibraryOperators.FROM_BASE64);
        register(SqlLibraryOperators.MD5);
        register(SqlLibraryOperators.SHA1);
        register(SqlStdOperatorTable.SUBSTRING);
        register(SqlLibraryOperators.LEFT);
        register(SqlLibraryOperators.RIGHT);
        register(SqlStdOperatorTable.REPLACE);
        register(SqlLibraryOperators.TRANSLATE3);
        register(SqlLibraryOperators.CHR);
        register(SqlStdOperatorTable.CHAR_LENGTH);
        register(SqlStdOperatorTable.CHARACTER_LENGTH);
        register(SqlStdOperatorTable.CONCAT);
        register(SqlLibraryOperators.CONCAT_FUNCTION);
        register(SqlStdOperatorTable.OVERLAY);
        register(SqlStdOperatorTable.POSITION);
        register(SqlStdOperatorTable.ASCII);
        register(SqlLibraryOperators.REPEAT);
        register(SqlLibraryOperators.SPACE);
        register(SqlLibraryOperators.STRCMP);
        register(SqlLibraryOperators.SOUNDEX);
        register(SqlLibraryOperators.DIFFERENCE);
        register(SqlLibraryOperators.REVERSE);
        register(SqlStdOperatorTable.TRIM);
        register(SqlLibraryOperators.LTRIM);
        register(SqlLibraryOperators.RTRIM);

        // Math functions.
        register(SqlStdOperatorTable.MOD); // Arithmetic remainder.
        register(SqlStdOperatorTable.EXP); // Euler's number e raised to the power of a value.
        register(SqlStdOperatorTable.POWER);
        register(SqlStdOperatorTable.LN); // Natural logarithm.
        register(SqlStdOperatorTable.LOG10); // The base 10 logarithm.
        register(SqlStdOperatorTable.ABS); // Absolute value.
        register(SqlStdOperatorTable.RAND); // Random.
        register(SqlStdOperatorTable.RAND_INTEGER); // Integer random.
        register(SqlStdOperatorTable.ACOS); // Arc cosine.
        register(SqlLibraryOperators.ACOSH); // Hyperbolic arc cosine.
        register(SqlStdOperatorTable.ASIN); // Arc sine.
        register(SqlLibraryOperators.ASINH); // Hyperbolic arc sine.
        register(SqlStdOperatorTable.ATAN); // Arc tangent.
        register(SqlStdOperatorTable.ATAN2); // Angle from coordinates.
        register(SqlLibraryOperators.ATANH); // Hyperbolic arc tangent.
        register(SqlStdOperatorTable.SQRT); // Square root.
        register(SqlStdOperatorTable.CBRT); // Cube root.
        register(SqlStdOperatorTable.COS); // Cosine
        register(SqlLibraryOperators.COSH); // Hyperbolic cosine.
        register(SqlStdOperatorTable.COT); // Cotangent.
        register(SqlLibraryOperators.COTH); // Hyperbolic cotangent.
        register(SqlStdOperatorTable.DEGREES); // Radians to degrees.
        register(SqlStdOperatorTable.RADIANS); // Degrees to radians.
        register(SqlStdOperatorTable.ROUND);
        register(SqlStdOperatorTable.SIGN);
        register(SqlStdOperatorTable.SIN); // Sine.
        register(SqlLibraryOperators.SINH); // Hyperbolic sine.
        register(SqlStdOperatorTable.TAN); // Tangent.
        register(SqlLibraryOperators.TANH); // Hyperbolic tangent.
        register(SqlLibraryOperators.SEC); // Secant.
        register(SqlLibraryOperators.SECH); // Hyperbolic secant.
        register(SqlLibraryOperators.CSC); // Cosecant.
        register(SqlLibraryOperators.CSCH); // Hyperbolic cosecant.
        register(SqlStdOperatorTable.TRUNCATE);
        register(SqlStdOperatorTable.PI);

        // Date and time.
        register(SqlStdOperatorTable.DATETIME_PLUS);
        register(SqlStdOperatorTable.MINUS_DATE);
        register(SqlStdOperatorTable.EXTRACT);
        register(SqlStdOperatorTable.FLOOR);
        register(SqlStdOperatorTable.CEIL);
        register(SqlStdOperatorTable.TIMESTAMP_ADD);
        register(SqlStdOperatorTable.TIMESTAMP_DIFF);
        register(SqlStdOperatorTable.LAST_DAY);
        register(SqlLibraryOperators.DAYNAME);
        register(SqlLibraryOperators.MONTHNAME);
        register(SqlStdOperatorTable.DAYOFMONTH);
        register(SqlStdOperatorTable.DAYOFWEEK);
        register(SqlStdOperatorTable.DAYOFYEAR);
        register(SqlStdOperatorTable.YEAR);
        register(SqlStdOperatorTable.QUARTER);
        register(SqlStdOperatorTable.MONTH);
        register(SqlStdOperatorTable.WEEK);
        register(SqlStdOperatorTable.HOUR);
        register(SqlStdOperatorTable.MINUTE);
        register(SqlStdOperatorTable.SECOND);
        register(SqlLibraryOperators.TIMESTAMP_SECONDS); // Seconds since 1970-01-01 to timestamp.
        register(SqlLibraryOperators.TIMESTAMP_MILLIS); // Milliseconds since 1970-01-01 to timestamp.
        register(SqlLibraryOperators.TIMESTAMP_MICROS); // Microseconds since 1970-01-01 to timestamp.
        register(SqlLibraryOperators.UNIX_SECONDS); // Timestamp to seconds since 1970-01-01.
        register(SqlLibraryOperators.UNIX_MILLIS); // Timestamp to milliseconds since 1970-01-01.
        register(SqlLibraryOperators.UNIX_MICROS); // Timestamp to microseconds since 1970-01-01.
        register(SqlLibraryOperators.UNIX_DATE); // Date to days since 1970-01-01.
        register(SqlLibraryOperators.DATE_FROM_UNIX_DATE); // Days since 1970-01-01 to date.
        register(SqlLibraryOperators.DATE); // String to date.
        register(SqlLibraryOperators.DATETIME); // String to datetime.
        register(SqlLibraryOperators.TIME); // String to time.

        // POSIX REGEX.
        register(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE);
        register(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE);
        register(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE);
        register(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE);
        register(SqlLibraryOperators.REGEXP_REPLACE);

        // Collections.
        register(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
        register(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);
        register(SqlStdOperatorTable.ITEM);
        register(SqlStdOperatorTable.CARDINALITY);
        register(SqlStdOperatorTable.IS_EMPTY);
        register(SqlStdOperatorTable.IS_NOT_EMPTY);

        register(SqlStdOperatorTable.MAP_QUERY);
        register(SqlStdOperatorTable.ARRAY_QUERY);

        // Multiset.
        // TODO https://issues.apache.org/jira/browse/IGNITE-15551
        //register(SqlStdOperatorTable.MULTISET_VALUE);
        //register(SqlStdOperatorTable.MULTISET_QUERY);
        //register(SqlStdOperatorTable.SLICE);
        //register(SqlStdOperatorTable.ELEMENT);
        //register(SqlStdOperatorTable.STRUCT_ACCESS);
        //register(SqlStdOperatorTable.MEMBER_OF);
        //register(SqlStdOperatorTable.IS_A_SET);
        //register(SqlStdOperatorTable.IS_NOT_A_SET);
        //register(SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT);
        //register(SqlStdOperatorTable.MULTISET_INTERSECT);
        //register(SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT);
        //register(SqlStdOperatorTable.MULTISET_EXCEPT);
        //register(SqlStdOperatorTable.MULTISET_UNION_DISTINCT);
        //register(SqlStdOperatorTable.MULTISET_UNION);
        //register(SqlStdOperatorTable.SUBMULTISET_OF);
        //register(SqlStdOperatorTable.NOT_SUBMULTISET_OF);

        // Other fuctions and operators.
        register(SqlStdOperatorTable.ROW);
        register(SqlStdOperatorTable.CAST);
        register(SqlLibraryOperators.INFIX_CAST);
        register(SqlStdOperatorTable.COALESCE);
        register(SqlLibraryOperators.NVL);
        register(SqlStdOperatorTable.NULLIF);
        register(SqlStdOperatorTable.CASE);
        register(SqlLibraryOperators.DECODE);
        register(SqlLibraryOperators.LEAST);
        register(SqlLibraryOperators.GREATEST);
        register(SqlLibraryOperators.COMPRESS);
        register(SqlStdOperatorTable.OCTET_LENGTH);
        register(SqlStdOperatorTable.DEFAULT);
        register(SqlStdOperatorTable.REINTERPRET);

        // XML Operators.
        register(SqlLibraryOperators.EXTRACT_VALUE);
        register(SqlLibraryOperators.XML_TRANSFORM);
        register(SqlLibraryOperators.EXTRACT_XML);
        register(SqlLibraryOperators.EXISTS_NODE);

        // JSON Operators
        register(SqlStdOperatorTable.JSON_VALUE_EXPRESSION);
        register(SqlStdOperatorTable.JSON_VALUE);
        register(SqlStdOperatorTable.JSON_QUERY);
        register(SqlLibraryOperators.JSON_TYPE);
        register(SqlStdOperatorTable.JSON_EXISTS);
        register(SqlLibraryOperators.JSON_DEPTH);
        register(SqlLibraryOperators.JSON_KEYS);
        register(SqlLibraryOperators.JSON_PRETTY);
        register(SqlLibraryOperators.JSON_LENGTH);
        register(SqlLibraryOperators.JSON_REMOVE);
        register(SqlLibraryOperators.JSON_STORAGE_SIZE);
        register(SqlStdOperatorTable.JSON_OBJECT);
        register(SqlStdOperatorTable.JSON_ARRAY);
        register(SqlStdOperatorTable.IS_JSON_VALUE);
        register(SqlStdOperatorTable.IS_JSON_OBJECT);
        register(SqlStdOperatorTable.IS_JSON_ARRAY);
        register(SqlStdOperatorTable.IS_JSON_SCALAR);
        register(SqlStdOperatorTable.IS_NOT_JSON_VALUE);
        register(SqlStdOperatorTable.IS_NOT_JSON_OBJECT);
        register(SqlStdOperatorTable.IS_NOT_JSON_ARRAY);
        register(SqlStdOperatorTable.IS_NOT_JSON_SCALAR);

        // Aggregate functions.
        register(SqlInternalOperators.LITERAL_AGG); // Internal operator, not implemented, required for serialization.

        // Current time functions.
        register(SqlStdOperatorTable.CURRENT_TIME);
        register(SqlStdOperatorTable.CURRENT_TIMESTAMP);
        register(SqlStdOperatorTable.CURRENT_DATE);
        register(SqlStdOperatorTable.LOCALTIME);
        register(SqlStdOperatorTable.LOCALTIMESTAMP);

        // Bit wise operations.
        register(SqlStdOperatorTable.BIT_AND);
        register(SqlStdOperatorTable.BIT_OR);
        register(SqlStdOperatorTable.BIT_XOR);
    }
}
