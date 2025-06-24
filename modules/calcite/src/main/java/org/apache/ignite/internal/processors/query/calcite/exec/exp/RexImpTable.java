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
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.OptimizeShuttle;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlTypeConstructorFunction;
import org.apache.calcite.sql.fun.SqlItemOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.adapter.enumerable.EnumUtils.generateCollatorExpression;
import static org.apache.calcite.linq4j.tree.ExpressionType.Add;
import static org.apache.calcite.linq4j.tree.ExpressionType.Divide;
import static org.apache.calcite.linq4j.tree.ExpressionType.Equal;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Multiply;
import static org.apache.calcite.linq4j.tree.ExpressionType.Negate;
import static org.apache.calcite.linq4j.tree.ExpressionType.NotEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Subtract;
import static org.apache.calcite.linq4j.tree.ExpressionType.UnaryPlus;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ACOSH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ASINH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ATANH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CHR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.COMPRESS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT_FUNCTION;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.COSH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.COTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CSC;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CSCH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATETIME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FROM_UNIX_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DAYNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DIFFERENCE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.EXISTS_NODE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.EXTRACT_VALUE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.EXTRACT_XML;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_DEPTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_KEYS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_PRETTY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_REMOVE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_STORAGE_SIZE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_TYPE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LEFT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LOG;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MD5;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MONTHNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REPEAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REVERSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RIGHT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RLIKE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SEC;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SECH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SHA1;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SINH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SOUNDEX;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.STRCMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TANH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_MICROS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_MILLIS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_SECONDS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_CHAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_MICROS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_MILLIS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_SECONDS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.XML_TRANSFORM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ABS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ACOS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ASCII;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ASIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN2;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CARDINALITY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CBRT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHECKED_DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHECKED_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHECKED_MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHECKED_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHECKED_UNARY_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DATETIME_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DEFAULT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DEGREES;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXTRACT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.INITCAP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_EMPTY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_SCALAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_EMPTY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_SCALAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_EXISTS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_QUERY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE_EXPRESSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST_DAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOG10;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOWER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OCTET_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OVERLAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POSITION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POWER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RADIANS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REINTERPRET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REPLACE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROUND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIMILAR_TO;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRIM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRUNCATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;
import static org.apache.calcite.util.ReflectUtil.isStatic;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.BITAND;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.BITOR;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.BITXOR;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.GREATEST2;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.LEAST2;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.NULL_BOUND;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.QUERY_ENGINE;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.SYSTEM_RANGE;
import static org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable.TYPEOF;

/**
 * Contains implementations of Rex operators as Java code.
 */
public class RexImpTable {
    /** */
    public static final RexImpTable INSTANCE = new RexImpTable();

    /** */
    public static final ConstantExpression NULL_EXPR = Expressions.constant(null);

    /** */
    public static final ConstantExpression FALSE_EXPR = Expressions.constant(false);

    /** */
    public static final ConstantExpression TRUE_EXPR = Expressions.constant(true);

    /** */
    public static final MemberExpression BOXED_FALSE_EXPR = Expressions.field(null, Boolean.class, "FALSE");

    /** */
    public static final MemberExpression BOXED_TRUE_EXPR = Expressions.field(null, Boolean.class, "TRUE");

    /** */
    private final Map<SqlOperator, RexCallImplementor> map = new HashMap<>();

    /** */
    RexImpTable() {
        defineMethod(ROW, BuiltInMethod.ARRAY.method, NullPolicy.NONE);
        defineMethod(UPPER, BuiltInMethod.UPPER.method, NullPolicy.STRICT);
        defineMethod(LOWER, BuiltInMethod.LOWER.method, NullPolicy.STRICT);
        defineMethod(INITCAP, BuiltInMethod.INITCAP.method, NullPolicy.STRICT);
        defineMethod(TO_BASE64, BuiltInMethod.TO_BASE64.method, NullPolicy.STRICT);
        defineMethod(FROM_BASE64, BuiltInMethod.FROM_BASE64.method, NullPolicy.STRICT);
        defineMethod(MD5, BuiltInMethod.MD5.method, NullPolicy.STRICT);
        defineMethod(SHA1, BuiltInMethod.SHA1.method, NullPolicy.STRICT);
        defineMethod(SUBSTRING, BuiltInMethod.SUBSTRING.method, NullPolicy.STRICT);
        defineMethod(LEFT, BuiltInMethod.LEFT.method, NullPolicy.ANY);
        defineMethod(RIGHT, BuiltInMethod.RIGHT.method, NullPolicy.ANY);
        define(REPLACE, new ReplaceImplementor());
        defineMethod(TRANSLATE3, BuiltInMethod.TRANSLATE3.method, NullPolicy.STRICT);
        defineMethod(CHR, BuiltInMethod.CHAR_FROM_UTF8.method, NullPolicy.STRICT);
        defineMethod(CHAR_LENGTH, BuiltInMethod.CHAR_LENGTH.method, NullPolicy.STRICT);
        defineMethod(OCTET_LENGTH, BuiltInMethod.OCTET_LENGTH.method, NullPolicy.STRICT);
        defineMethod(CONCAT, BuiltInMethod.STRING_CONCAT.method, NullPolicy.STRICT);
        defineMethod(CONCAT_FUNCTION, BuiltInMethod.MULTI_STRING_CONCAT.method, NullPolicy.STRICT);
        defineMethod(OVERLAY, BuiltInMethod.OVERLAY.method, NullPolicy.STRICT);
        defineMethod(POSITION, BuiltInMethod.POSITION.method, NullPolicy.STRICT);
        defineMethod(ASCII, BuiltInMethod.ASCII.method, NullPolicy.STRICT);
        defineMethod(REPEAT, BuiltInMethod.REPEAT.method, NullPolicy.STRICT);
        defineMethod(SPACE, BuiltInMethod.SPACE.method, NullPolicy.STRICT);
        defineMethod(STRCMP, BuiltInMethod.STRCMP.method, NullPolicy.STRICT);
        defineMethod(SOUNDEX, BuiltInMethod.SOUNDEX.method, NullPolicy.STRICT);
        defineMethod(DIFFERENCE, BuiltInMethod.DIFFERENCE.method, NullPolicy.STRICT);
        defineMethod(REVERSE, BuiltInMethod.REVERSE.method, NullPolicy.STRICT);

        map.put(TRIM, new TrimImplementor());

        // logical
        map.put(AND, new LogicalAndImplementor());
        map.put(OR, new LogicalOrImplementor());
        map.put(NOT, new LogicalNotImplementor());

        // comparisons
        defineBinary(LESS_THAN, LessThan, NullPolicy.STRICT, "lt");
        defineBinary(LESS_THAN_OR_EQUAL, LessThanOrEqual, NullPolicy.STRICT, "le");
        defineBinary(GREATER_THAN, GreaterThan, NullPolicy.STRICT, "gt");
        defineBinary(GREATER_THAN_OR_EQUAL, GreaterThanOrEqual, NullPolicy.STRICT,
            "ge");
        defineBinary(EQUALS, Equal, NullPolicy.STRICT, "eq");
        defineBinary(NOT_EQUALS, NotEqual, NullPolicy.STRICT, "ne");

        // arithmetic
        defineBinary(PLUS, Add, NullPolicy.STRICT, "plus");
        defineBinary(MINUS, Subtract, NullPolicy.STRICT, "minus");
        defineBinary(MULTIPLY, Multiply, NullPolicy.STRICT, "multiply");
        defineBinary(DIVIDE, Divide, NullPolicy.STRICT, "divide");
        defineBinary(DIVIDE_INTEGER, Divide, NullPolicy.STRICT, "divide");
        defineUnary(UNARY_MINUS, Negate, NullPolicy.STRICT,
            BuiltInMethod.BIG_DECIMAL_NEGATE.getMethodName());
        defineUnary(UNARY_PLUS, UnaryPlus, NullPolicy.STRICT, null);

        defineMethod(MOD, "mod", NullPolicy.STRICT);
        defineMethod(EXP, "exp", NullPolicy.STRICT);
        defineMethod(POWER, "power", NullPolicy.STRICT);
        defineMethod(ABS, "abs", NullPolicy.STRICT);

        map.put(LN, new LogImplementor());
        map.put(LOG, new LogImplementor());
        map.put(LOG10, new LogImplementor());

        defineReflective(RAND, BuiltInMethod.RAND.method,
            BuiltInMethod.RAND_SEED.method);
        defineReflective(RAND_INTEGER, BuiltInMethod.RAND_INTEGER.method,
            BuiltInMethod.RAND_INTEGER_SEED.method);

        defineMethod(ACOS, BuiltInMethod.ACOS.method, NullPolicy.STRICT);
        defineMethod(ACOSH, BuiltInMethod.ACOSH.method, NullPolicy.STRICT);
        defineMethod(ASIN, BuiltInMethod.ASIN.method, NullPolicy.STRICT);
        defineMethod(ASINH, BuiltInMethod.ASINH.method, NullPolicy.STRICT);
        defineMethod(ATAN, BuiltInMethod.ATAN.method, NullPolicy.STRICT);
        defineMethod(ATAN2, BuiltInMethod.ATAN2.method, NullPolicy.STRICT);
        defineMethod(ATANH, BuiltInMethod.ATANH.method, NullPolicy.STRICT);
        defineMethod(CBRT, BuiltInMethod.CBRT.method, NullPolicy.STRICT);
        defineMethod(COS, BuiltInMethod.COS.method, NullPolicy.STRICT);
        defineMethod(COSH, BuiltInMethod.COSH.method, NullPolicy.STRICT);
        defineMethod(COT, BuiltInMethod.COT.method, NullPolicy.STRICT);
        defineMethod(COTH, BuiltInMethod.COTH.method, NullPolicy.STRICT);
        defineMethod(CSC, BuiltInMethod.CSC.method, NullPolicy.STRICT);
        defineMethod(CSCH, BuiltInMethod.CSCH.method, NullPolicy.STRICT);
        defineMethod(DEGREES, BuiltInMethod.DEGREES.method, NullPolicy.STRICT);
        defineMethod(RADIANS, BuiltInMethod.RADIANS.method, NullPolicy.STRICT);
        defineMethod(ROUND, BuiltInMethod.SROUND.method, NullPolicy.STRICT);
        defineMethod(SEC, BuiltInMethod.SEC.method, NullPolicy.STRICT);
        defineMethod(SECH, BuiltInMethod.SECH.method, NullPolicy.STRICT);
        defineMethod(SIGN, BuiltInMethod.SIGN.method, NullPolicy.STRICT);
        defineMethod(SIN, BuiltInMethod.SIN.method, NullPolicy.STRICT);
        defineMethod(SINH, BuiltInMethod.SINH.method, NullPolicy.STRICT);
        defineMethod(TAN, BuiltInMethod.TAN.method, NullPolicy.STRICT);
        defineMethod(TANH, BuiltInMethod.TANH.method, NullPolicy.STRICT);
        defineMethod(TRUNCATE, BuiltInMethod.STRUNCATE.method, NullPolicy.STRICT);

        map.put(PI, new PiImplementor());

        // datetime
        map.put(DATETIME_PLUS, new DatetimeArithmeticImplementor());
        map.put(MINUS_DATE, new DatetimeArithmeticImplementor());
        map.put(EXTRACT, new ExtractImplementor());
        map.put(FLOOR,
            new FloorImplementor(BuiltInMethod.FLOOR.method.getName(),
                BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
                BuiltInMethod.UNIX_DATE_FLOOR.method));
        map.put(CEIL,
            new FloorImplementor(BuiltInMethod.CEIL.method.getName(),
                BuiltInMethod.UNIX_TIMESTAMP_CEIL.method,
                BuiltInMethod.UNIX_DATE_CEIL.method));

        map.put(LAST_DAY,
                new LastDayImplementor("lastDay", BuiltInMethod.LAST_DAY));
        map.put(DAYNAME,
            new PeriodNameImplementor("dayName",
                BuiltInMethod.DAYNAME_WITH_TIMESTAMP,
                BuiltInMethod.DAYNAME_WITH_DATE));
        map.put(MONTHNAME,
            new PeriodNameImplementor("monthName",
                BuiltInMethod.MONTHNAME_WITH_TIMESTAMP,
                BuiltInMethod.MONTHNAME_WITH_DATE));
        defineMethod(TIMESTAMP_SECONDS, "timestampSeconds", NullPolicy.STRICT);
        defineMethod(TIMESTAMP_MILLIS, "timestampMillis", NullPolicy.STRICT);
        defineMethod(TIMESTAMP_MICROS, "timestampMicros", NullPolicy.STRICT);
        defineMethod(UNIX_SECONDS, "unixSeconds", NullPolicy.STRICT);
        defineMethod(UNIX_MILLIS, "unixMillis", NullPolicy.STRICT);
        defineMethod(UNIX_MICROS, "unixMicros", NullPolicy.STRICT);
        defineMethod(DATE_FROM_UNIX_DATE, "dateFromUnixDate", NullPolicy.STRICT);
        defineMethod(UNIX_DATE, "unixDate", NullPolicy.STRICT);
        defineMethod(DATE, "date", NullPolicy.STRICT);
        defineMethod(DATETIME, "datetime", NullPolicy.STRICT);
        defineMethod(TIME, "time", NullPolicy.STRICT);
        defineReflective(TO_CHAR, BuiltInMethod.TO_CHAR.method);
        defineReflective(TO_DATE, BuiltInMethod.TO_DATE.method);
        defineReflective(TO_TIMESTAMP, BuiltInMethod.TO_TIMESTAMP.method);

        map.put(IS_NULL, new IsNullImplementor());
        map.put(IS_NOT_NULL, new IsNotNullImplementor());
        map.put(IS_TRUE, new IsTrueImplementor());
        map.put(IS_NOT_TRUE, new IsNotTrueImplementor());
        map.put(IS_FALSE, new IsFalseImplementor());
        map.put(IS_NOT_FALSE, new IsNotFalseImplementor());

        // LIKE and SIMILAR
        defineReflective(LIKE, BuiltInMethod.LIKE.method,
            BuiltInMethod.LIKE_ESCAPE.method);
        defineReflective(ILIKE, BuiltInMethod.ILIKE.method,
            BuiltInMethod.ILIKE_ESCAPE.method);
        defineReflective(RLIKE, BuiltInMethod.RLIKE.method);
        defineReflective(SIMILAR_TO, BuiltInMethod.SIMILAR.method,
            BuiltInMethod.SIMILAR_ESCAPE.method);

        // POSIX REGEX
        ReflectiveImplementor insensitiveImplementor =
            defineReflective(POSIX_REGEX_CASE_INSENSITIVE,
                BuiltInMethod.POSIX_REGEX_INSENSITIVE.method);
        ReflectiveImplementor sensitiveImplementor =
            defineReflective(POSIX_REGEX_CASE_SENSITIVE,
                BuiltInMethod.POSIX_REGEX_SENSITIVE.method);
        map.put(NEGATED_POSIX_REGEX_CASE_INSENSITIVE,
            NotImplementor.of(insensitiveImplementor));
        map.put(NEGATED_POSIX_REGEX_CASE_SENSITIVE,
            NotImplementor.of(sensitiveImplementor));
        defineReflective(REGEXP_REPLACE_3,
            BuiltInMethod.REGEXP_REPLACE3.method,
            BuiltInMethod.REGEXP_REPLACE4.method,
            BuiltInMethod.REGEXP_REPLACE5_OCCURRENCE.method,
            BuiltInMethod.REGEXP_REPLACE6.method);

        // Multisets & arrays
        defineMethod(CARDINALITY, BuiltInMethod.COLLECTION_SIZE.method,
            NullPolicy.STRICT);
        final MethodImplementor isEmptyImplementor =
            new MethodImplementor(BuiltInMethod.IS_EMPTY.method, NullPolicy.NONE,
                false);
        map.put(IS_EMPTY, isEmptyImplementor);
        map.put(IS_NOT_EMPTY, NotImplementor.of(isEmptyImplementor));

        // TODO https://issues.apache.org/jira/browse/IGNITE-15551
/*
        defineMethod(SLICE, BuiltInMethod.SLICE.method, NullPolicy.NONE);
        defineMethod(ELEMENT, BuiltInMethod.ELEMENT.method, NullPolicy.STRICT);
        defineMethod(STRUCT_ACCESS, BuiltInMethod.STRUCT_ACCESS.method, NullPolicy.ANY);
        defineMethod(MEMBER_OF, BuiltInMethod.MEMBER_OF.method, NullPolicy.NONE);
        final MethodImplementor isASetImplementor =
            new MethodImplementor(BuiltInMethod.IS_A_SET.method, NullPolicy.NONE,
                false);
        map.put(IS_A_SET, isASetImplementor);
        map.put(IS_NOT_A_SET, NotImplementor.of(isASetImplementor));
        defineMethod(MULTISET_INTERSECT_DISTINCT,
            BuiltInMethod.MULTISET_INTERSECT_DISTINCT.method, NullPolicy.NONE);
        defineMethod(MULTISET_INTERSECT,
            BuiltInMethod.MULTISET_INTERSECT_ALL.method, NullPolicy.NONE);
        defineMethod(MULTISET_EXCEPT_DISTINCT,
            BuiltInMethod.MULTISET_EXCEPT_DISTINCT.method, NullPolicy.NONE);
        defineMethod(MULTISET_EXCEPT, BuiltInMethod.MULTISET_EXCEPT_ALL.method, NullPolicy.NONE);
        defineMethod(MULTISET_UNION_DISTINCT,
            BuiltInMethod.MULTISET_UNION_DISTINCT.method, NullPolicy.NONE);
        defineMethod(MULTISET_UNION, BuiltInMethod.MULTISET_UNION_ALL.method, NullPolicy.NONE);
        final MethodImplementor subMultisetImplementor =
            new MethodImplementor(BuiltInMethod.SUBMULTISET_OF.method, NullPolicy.NONE, false);
        map.put(SUBMULTISET_OF, subMultisetImplementor);
        map.put(NOT_SUBMULTISET_OF, NotImplementor.of(subMultisetImplementor));
*/

        map.put(COALESCE, new CoalesceImplementor());
        map.put(CAST, new CastImplementor());

        map.put(REINTERPRET, new ReinterpretImplementor());

        final RexCallImplementor val = new ValueConstructorImplementor();
        map.put(MAP_VALUE_CONSTRUCTOR, val);
        map.put(ARRAY_VALUE_CONSTRUCTOR, val);
        map.put(ITEM, new ItemImplementor());

        map.put(DEFAULT, new DefaultImplementor());

        // Compression Operators
        defineMethod(COMPRESS, BuiltInMethod.COMPRESS.method, NullPolicy.ARG0);

        // Xml Operators
        defineMethod(EXTRACT_VALUE, BuiltInMethod.EXTRACT_VALUE.method, NullPolicy.ARG0);
        defineMethod(XML_TRANSFORM, BuiltInMethod.XML_TRANSFORM.method, NullPolicy.ARG0);
        defineMethod(EXTRACT_XML, BuiltInMethod.EXTRACT_XML.method, NullPolicy.ARG0);
        defineMethod(EXISTS_NODE, BuiltInMethod.EXISTS_NODE.method, NullPolicy.ARG0);

        // Json Operators
        defineMethod(JSON_VALUE_EXPRESSION,
            BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT);
        defineReflective(JSON_EXISTS, BuiltInMethod.JSON_EXISTS2.method,
            BuiltInMethod.JSON_EXISTS3.method);
        map.put(JSON_VALUE,
            new JsonValueImplementor(BuiltInMethod.JSON_VALUE.method));
        map.put(JSON_QUERY, new JsonQueryImplementor(BuiltInMethod.JSON_QUERY.method));
        defineMethod(JSON_TYPE, BuiltInMethod.JSON_TYPE.method, NullPolicy.ARG0);
        defineMethod(JSON_DEPTH, BuiltInMethod.JSON_DEPTH.method, NullPolicy.ARG0);
        defineMethod(JSON_KEYS, BuiltInMethod.JSON_KEYS.method, NullPolicy.ARG0);
        defineMethod(JSON_PRETTY, BuiltInMethod.JSON_PRETTY.method, NullPolicy.ARG0);
        defineMethod(JSON_LENGTH, BuiltInMethod.JSON_LENGTH.method, NullPolicy.ARG0);
        defineMethod(JSON_REMOVE, BuiltInMethod.JSON_REMOVE.method, NullPolicy.ARG0);
        defineMethod(JSON_STORAGE_SIZE, BuiltInMethod.JSON_STORAGE_SIZE.method, NullPolicy.ARG0);
        defineMethod(JSON_OBJECT, BuiltInMethod.JSON_OBJECT.method, NullPolicy.NONE);
        defineMethod(JSON_ARRAY, BuiltInMethod.JSON_ARRAY.method, NullPolicy.NONE);
        map.put(IS_JSON_VALUE,
            new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method,
                NullPolicy.NONE, false));
        map.put(IS_JSON_OBJECT,
            new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method,
                NullPolicy.NONE, false));
        map.put(IS_JSON_ARRAY,
            new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method,
                NullPolicy.NONE, false));
        map.put(IS_JSON_SCALAR,
            new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method,
                NullPolicy.NONE, false));
        map.put(IS_NOT_JSON_VALUE,
            NotImplementor.of(
                new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method,
                    NullPolicy.NONE, false)));
        map.put(IS_NOT_JSON_OBJECT,
            NotImplementor.of(
                new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method,
                    NullPolicy.NONE, false)));
        map.put(IS_NOT_JSON_ARRAY,
            NotImplementor.of(
                new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method,
                    NullPolicy.NONE, false)));
        map.put(IS_NOT_JSON_SCALAR,
            NotImplementor.of(
                new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method,
                    NullPolicy.NONE, false)));

        // System functions.
        final SystemFunctionImplementor sysFunctionImplementor = new SystemFunctionImplementor();
        map.put(SYSTEM_RANGE, sysFunctionImplementor);

        // Current time functions.
        map.put(CURRENT_TIME, sysFunctionImplementor);
        map.put(CURRENT_TIMESTAMP, sysFunctionImplementor);
        map.put(CURRENT_DATE, sysFunctionImplementor);
        map.put(LOCALTIME, sysFunctionImplementor);
        map.put(LOCALTIMESTAMP, sysFunctionImplementor);

        map.put(TYPEOF, sysFunctionImplementor);
        map.put(QUERY_ENGINE, sysFunctionImplementor);
        map.put(NULL_BOUND, sysFunctionImplementor);

        defineMethod(LEAST2, IgniteMethod.LEAST2.method(), NullPolicy.ALL);
        defineMethod(GREATEST2, IgniteMethod.GREATEST2.method(), NullPolicy.ALL);

        // Operator IS_NOT_DISTINCT_FROM is removed by RexSimplify, but still possible in join conditions, so
        // implementation required.
        defineMethod(IS_NOT_DISTINCT_FROM, IgniteMethod.IS_NOT_DISTINCT_FROM.method(), NullPolicy.NONE);

        defineMethod(BITAND, BuiltInMethod.BIT_AND.method, NullPolicy.ANY);
        defineMethod(BITOR, BuiltInMethod.BIT_OR.method, NullPolicy.ANY);
        defineMethod(BITXOR, BuiltInMethod.BIT_XOR.method, NullPolicy.ANY);
    }

    /** */
    public void define(SqlOperator operator, RexCallImplementor implementor) {
        map.put(operator, implementor);
    }

    /** */
    private void defineMethod(SqlOperator operator, String functionName, NullPolicy nullPolicy) {
        map.put(operator, new MethodNameImplementor(functionName, nullPolicy, false));
    }

    /** */
    public void defineMethod(SqlOperator operator, Method method, NullPolicy nullPolicy) {
        map.put(operator, new MethodImplementor(method, nullPolicy, false));
    }

    /** */
    public ReflectiveImplementor defineReflective(SqlOperator operator, Method... methods) {
        final ReflectiveImplementor implementor = new ReflectiveImplementor(ImmutableList.copyOf(methods));
        map.put(operator, implementor);
        return implementor;
    }

    /** */
    private void defineUnary(SqlOperator operator, ExpressionType expressionType,
        NullPolicy nullPolicy, String backupMethodName) {
        map.put(operator, new UnaryImplementor(expressionType, nullPolicy, backupMethodName));
    }

    /** */
    private void defineBinary(SqlOperator operator, ExpressionType expressionType,
        NullPolicy nullPolicy, String backupMethodName) {
        map.put(operator,
            new BinaryImplementor(nullPolicy, true, expressionType,
                backupMethodName));
    }

    /** */
    private static RexCallImplementor wrapAsRexCallImplementor(
        final CallImplementor implementor) {
        return new AbstractRexCallImplementor("udf", NullPolicy.NONE, false) {
            @Override Expression implementSafe(RexToLixTranslator translator,
                RexCall call, List<Expression> argValueList) {
                return implementor.implement(translator, call, RexImpTable.NullAs.NULL);
            }
        };
    }

    /** */
    public RexCallImplementor get(final SqlOperator operator) {
        if (operator instanceof SqlUserDefinedFunction) {
            org.apache.calcite.schema.Function udf =
                ((SqlUserDefinedFunction)operator).getFunction();
            if (!(udf instanceof ImplementableFunction)) {
                throw new IllegalStateException("User defined function " + operator
                    + " must implement ImplementableFunction");
            }
            CallImplementor implementor = ((ImplementableFunction)udf).getImplementor();
            return wrapAsRexCallImplementor(implementor);
        }
        else if (operator instanceof SqlTypeConstructorFunction)
            return map.get(SqlStdOperatorTable.ROW);

        return map.get(operator);
    }

    /** */
    static Expression optimize(Expression expression) {
        return expression.accept(new OptimizeShuttle());
    }

    /** */
    static Expression optimize2(Expression operand, Expression expression) {
        if (Primitive.is(operand.getType())) {
            // Primitive values cannot be null
            return optimize(expression);
        }

        return optimize(
            Expressions.condition(
                Expressions.equal(operand, NULL_EXPR),
                NULL_EXPR,
                expression));
    }

    /** */
    private static RelDataType toSql(RelDataTypeFactory typeFactory,
        RelDataType type) {
        if (type instanceof RelDataTypeFactoryImpl.JavaType) {
            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName != null && typeName != SqlTypeName.OTHER) {
                return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(typeName),
                    type.isNullable());
            }
        }
        return type;
    }

    /** */
    private static <E> boolean allSame(List<E> list) {
        E prev = null;
        for (E e : list) {
            if (prev != null && !prev.equals(e))
                return false;

            prev = e;
        }
        return true;
    }

    /**
     * Strategy what an operator should return if one of its arguments is null.
     */
    public enum NullAs {
        /**
         * The most common policy among the SQL built-in operators. If one of the arguments is null, returns null.
         */
        NULL,

        /**
         * If one of the arguments is null, the function returns false. Example: {@code IS NOT NULL}.
         */
        FALSE,

        /**
         * If one of the arguments is null, the function returns true. Example: {@code IS NULL}.
         */
        TRUE,

        /**
         * It is not possible for any of the arguments to be null.  If the argument type is nullable, the enclosing code
         * will already have performed a not-null check. This may allow the operator implementor to generate a more
         * efficient implementation, for example, by avoiding boxing or unboxing.
         */
        NOT_POSSIBLE,

        /** Return false if result is not null, true if result is null. */
        IS_NULL,

        /** Return true if result is not null, false if result is null. */
        IS_NOT_NULL;

        /** */
        public static NullAs of(boolean nullable) {
            return nullable ? NULL : NOT_POSSIBLE;
        }

        /**
         * Adapts an expression with "normal" result to one that adheres to this particular policy.
         */
        public Expression handle(Expression x) {
            switch (Primitive.flavor(x.getType())) {
                case PRIMITIVE:
                    // Expression cannot be null. We can skip any runtime checks.
                    switch (this) {
                        case NULL:
                        case NOT_POSSIBLE:
                        case FALSE:
                        case TRUE:
                            return x;
                        case IS_NULL:
                            return FALSE_EXPR;
                        case IS_NOT_NULL:
                            return TRUE_EXPR;
                        default:
                            throw new AssertionError();
                    }
                case BOX:
                    switch (this) {
                        case NOT_POSSIBLE:
                            return ConverterUtils.convert(x,
                                Primitive.ofBox(x.getType()).primitiveClass);
                    }
                    // fall through
            }
            switch (this) {
                case NULL:
                case NOT_POSSIBLE:
                    return x;
                case FALSE:
                    return Expressions.call(BuiltInMethod.IS_TRUE.method, x);
                case TRUE:
                    return Expressions.call(BuiltInMethod.IS_NOT_FALSE.method, x);
                case IS_NULL:
                    return Expressions.equal(x, NULL_EXPR);
                case IS_NOT_NULL:
                    return Expressions.notEqual(x, NULL_EXPR);
                default:
                    throw new AssertionError();
            }
        }
    }

    /** */
    static Expression getDefaultValue(Type type) {
        if (Primitive.is(type)) {
            Primitive p = Primitive.of(type);
            return Expressions.constant(p.defaultValue, type);
        }
        return Expressions.constant(null, type);
    }

    /**
     * Multiplies an expression by a constant and divides by another constant, optimizing appropriately.
     *
     * <p>For example, {@code multiplyDivide(e, 10, 1000)} returns
     * {@code e / 100}.
     */
    public static Expression multiplyDivide(Expression e, BigDecimal multiplier,
        BigDecimal divider) {
        if (multiplier.equals(BigDecimal.ONE)) {
            if (divider.equals(BigDecimal.ONE))
                return e;

            return Expressions.divide(e,
                Expressions.constant(divider.intValueExact()));
        }
        final BigDecimal x =
            multiplier.divide(divider, RoundingMode.UNNECESSARY);
        switch (x.compareTo(BigDecimal.ONE)) {
            case 0:
                return e;
            case 1:
                return Expressions.multiply(e, Expressions.constant(x.intValueExact()));
            case -1:
                return multiplyDivide(e, BigDecimal.ONE, x);
            default:
                throw new AssertionError();
        }
    }

    /** Implementor for the {@code LAST_DAY} function. */
    private static class LastDayImplementor extends MethodNameImplementor {
        /** */
        private final BuiltInMethod dateMethod;

        /** */
        LastDayImplementor(String methodName, BuiltInMethod dateMethod) {
            super(methodName, methodName, NullPolicy.STRICT, false);
            this.dateMethod = dateMethod;
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
                                 RexCall call, List<Expression> argValueList) {
            Expression operand = argValueList.get(0);
            final RelDataType type = call.operands.get(0).getType();
            switch (type.getSqlTypeName()) {
                case TIMESTAMP:
                    operand =
                            Expressions.call(BuiltInMethod.TIMESTAMP_TO_DATE.method, operand);
                    // fall through
                case DATE:
                    return Expressions.call(dateMethod.method.getDeclaringClass(),
                            dateMethod.method.getName(), operand);
                default:
                    throw new AssertionError("unknown type " + type);
            }
        }
    }

    /** Implementor for the {@code TRIM} function. */
    private static class TrimImplementor extends AbstractRexCallImplementor {
        /** */
        TrimImplementor() {
            super("trim", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final boolean strict = !translator.conformance.allowExtendedTrim();
            final Object val = translator.getLiteralValue(argValueList.get(0));
            SqlTrimFunction.Flag flag = (SqlTrimFunction.Flag)val;
            return Expressions.call(
                BuiltInMethod.TRIM.method,
                Expressions.constant(
                    flag == SqlTrimFunction.Flag.BOTH
                        || flag == SqlTrimFunction.Flag.LEADING),
                Expressions.constant(
                    flag == SqlTrimFunction.Flag.BOTH
                        || flag == SqlTrimFunction.Flag.TRAILING),
                argValueList.get(1),
                argValueList.get(2),
                Expressions.constant(strict));
        }
    }

    /**
     * Implementor for the {@code MONTHNAME} and {@code DAYNAME} functions. Each takes a {@link java.util.Locale}
     * argument.
     */
    private static class PeriodNameImplementor extends MethodNameImplementor {
        /** */
        private final BuiltInMethod timestampMethod;

        /** */
        private final BuiltInMethod dateMethod;

        /** */
        PeriodNameImplementor(String methodName, BuiltInMethod timestampMethod,
            BuiltInMethod dateMethod) {
            super(methodName, NullPolicy.STRICT, false);
            this.timestampMethod = timestampMethod;
            this.dateMethod = dateMethod;
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            Expression operand = argValueList.get(0);
            final RelDataType type = call.operands.get(0).getType();
            switch (type.getSqlTypeName()) {
                case TIMESTAMP:
                    return getExpression(translator, operand, timestampMethod);
                case DATE:
                    return getExpression(translator, operand, dateMethod);
                default:
                    throw new AssertionError("unknown type " + type);
            }
        }

        /** */
        protected Expression getExpression(RexToLixTranslator translator,
            Expression operand, BuiltInMethod builtInMethod) {
            final MethodCallExpression locale =
                Expressions.call(BuiltInMethod.LOCALE.method, translator.getRoot());
            return Expressions.call(builtInMethod.method.getDeclaringClass(),
                builtInMethod.method.getName(), operand, locale);
        }
    }

    /** Implementor for the {@code FLOOR} and {@code CEIL} functions. */
    private static class FloorImplementor extends MethodNameImplementor {
        /** */
        final Method timestampMethod;

        /** */
        final Method dateMethod;

        /** */
        FloorImplementor(String methodName, Method timestampMethod, Method dateMethod) {
            super(methodName, NullPolicy.STRICT, false);
            this.timestampMethod = timestampMethod;
            this.dateMethod = dateMethod;
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            switch (call.getOperands().size()) {
                case 1:
                    switch (call.getType().getSqlTypeName()) {
                        case BIGINT:
                        case INTEGER:
                        case SMALLINT:
                        case TINYINT:
                            return argValueList.get(0);
                        default:
                            return super.implementSafe(translator, call, argValueList);
                    }

                case 2:
                    final Type type;
                    final Method floorMethod;
                    final boolean preFloor;
                    Expression operand = argValueList.get(0);
                    switch (call.getType().getSqlTypeName()) {
                        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                            operand = Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
                            // fall through
                        case TIMESTAMP:
                            type = long.class;
                            floorMethod = timestampMethod;
                            preFloor = true;
                            break;
                        default:
                            type = int.class;
                            floorMethod = dateMethod;
                            preFloor = false;
                    }
                    final TimeUnitRange timeUnitRange =
                        (TimeUnitRange)translator.getLiteralValue(argValueList.get(1));
                    switch (timeUnitRange) {
                        case YEAR:
                        case QUARTER:
                        case MONTH:
                        case WEEK:
                        case DAY:
                            final Expression operand1 =
                                preFloor ? call(operand, type, TimeUnit.DAY) : operand;
                            return Expressions.call(floorMethod,
                                translator.getLiteral(argValueList.get(1)), operand1);
                        case NANOSECOND:
                        default:
                            return call(operand, type, timeUnitRange.startUnit);
                    }

                default:
                    throw new AssertionError();
            }
        }

        /** */
        private Expression call(Expression operand, Type type,
            TimeUnit timeUnit) {
            return Expressions.call(SqlFunctions.class, methodName,
                ConverterUtils.convert(operand, type),
                ConverterUtils.convert(
                    Expressions.constant(timeUnit.multiplier), type));
        }
    }

    /** Implementor for a function that generates calls to a given method. */
    private static class MethodImplementor extends AbstractRexCallImplementor {
        /** */
        protected final Method method;

        /** */
        MethodImplementor(Method method, NullPolicy nullPolicy, boolean harmonize) {
            super("method_call", nullPolicy, harmonize);
            this.method = method;
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
            RexCall call, List<Expression> argValueList) {
            final Expression expression;
            Class clazz = method.getDeclaringClass();
            if (Modifier.isStatic(method.getModifiers()))
                expression = EnumUtils.call(null, clazz, method.getName(), argValueList);
            else {
                expression = EnumUtils.call(argValueList.get(0), clazz, method.getName(),
                    Util.skip(argValueList, 1));
            }
            return expression;
        }
    }

    /**
     * Implementor for JSON_QUERY function. Passes the jsonize flag depending on the output type.
     */
    private static class JsonQueryImplementor extends MethodImplementor {
        /** */
        private JsonQueryImplementor(Method method) {
            super(method, NullPolicy.ARG0, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator, RexCall call, List<Expression> argValList) {
            List<Expression> newOperands = new ArrayList<>(argValList);

            Expression jsonize = SqlTypeUtil.inCharFamily(call.getType()) ? TRUE_EXPR : FALSE_EXPR;

            newOperands.add(jsonize);

            List<Expression> argValList0 = ConverterUtils.fromInternal(method.getParameterTypes(), newOperands);

            Expression target = Expressions.new_(method.getDeclaringClass());

            return Expressions.call(target, method, argValList0);
        }
    }

    /**
     * Implementor for JSON_VALUE function, convert to solid format
     * "JSON_VALUE(json_doc, path, empty_behavior, empty_default, error_behavior, error default)"
     * in order to simplify the runtime implementation.
     *
     * <p>We should avoid this when we support
     * variable arguments function.
     */
    private static class JsonValueImplementor extends MethodImplementor {
        /** */
        JsonValueImplementor(Method method) {
            super(method, NullPolicy.ARG0, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
            RexCall call, List<Expression> argValueList) {
            final List<Expression> newOperands = new ArrayList<>();
            newOperands.add(argValueList.get(0));
            newOperands.add(argValueList.get(1));
            List<Expression> leftExprs = Util.skip(argValueList, 2);
            // Default value for JSON_VALUE behaviors.
            Expression emptyBehavior = Expressions.constant(SqlJsonValueEmptyOrErrorBehavior.NULL);
            Expression dfltValOnEmpty = Expressions.constant(null);
            Expression errorBehavior = Expressions.constant(SqlJsonValueEmptyOrErrorBehavior.NULL);
            Expression dfltValOnError = Expressions.constant(null);
            // Patched up with user defines.
            if (!leftExprs.isEmpty()) {
                for (int i = 0; i < leftExprs.size(); i++) {
                    Expression expr = leftExprs.get(i);
                    final Object exprVal = translator.getLiteralValue(expr);
                    if (exprVal != null) {
                        int dfltSymbolIdx = i - 2;
                        if (exprVal == SqlJsonEmptyOrError.EMPTY) {
                            if (dfltSymbolIdx >= 0
                                && translator.getLiteralValue(leftExprs.get(dfltSymbolIdx))
                                == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
                                dfltValOnEmpty = leftExprs.get(i - 1);
                                emptyBehavior = leftExprs.get(dfltSymbolIdx);
                            }
                            else
                                emptyBehavior = leftExprs.get(i - 1);
                        }
                        else if (exprVal == SqlJsonEmptyOrError.ERROR) {
                            if (dfltSymbolIdx >= 0
                                && translator.getLiteralValue(leftExprs.get(dfltSymbolIdx))
                                == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
                                dfltValOnError = leftExprs.get(i - 1);
                                errorBehavior = leftExprs.get(dfltSymbolIdx);
                            }
                            else
                                errorBehavior = leftExprs.get(i - 1);
                        }
                    }
                }
            }
            newOperands.add(emptyBehavior);
            newOperands.add(dfltValOnEmpty);
            newOperands.add(errorBehavior);
            newOperands.add(dfltValOnError);
            List<Expression> argValList0 =
                ConverterUtils.fromInternal(method.getParameterTypes(), newOperands);
            final Expression target =
                Expressions.new_(method.getDeclaringClass());
            return Expressions.call(target, method, argValList0);
        }
    }

    /**
     * Implementor for SQL functions that generates calls to a given method name.
     *
     * <p>Use this, as opposed to {@link MethodImplementor}, if the SQL function
     * is overloaded; then you can use one implementor for several overloads.
     */
    private static class MethodNameImplementor extends AbstractRexCallImplementor {
        /** */
        protected final String methodName;

        /** */
        MethodNameImplementor(String methodName, NullPolicy nullPolicy, boolean harmonize) {
            this("method_name_call", methodName, nullPolicy, harmonize);
        }

        /** */
        MethodNameImplementor(
            String variableName,
            String methodName,
            NullPolicy nullPolicy,
            boolean harmonize
        ) {
            super(variableName, nullPolicy, harmonize);
            this.methodName = methodName;
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
            RexCall call, List<Expression> argValueList) {
            return EnumUtils.call(
                null,
                SqlFunctions.class,
                methodName,
                argValueList);
        }
    }

    /** Implementor for binary operators. */
    private static class BinaryImplementor extends AbstractRexCallImplementor {
        /**
         * Types that can be arguments to comparison operators such as {@code <}.
         */
        private static final List<Primitive> COMP_OP_TYPES =
            ImmutableList.of(
                Primitive.BYTE,
                Primitive.CHAR,
                Primitive.SHORT,
                Primitive.INT,
                Primitive.LONG,
                Primitive.FLOAT,
                Primitive.DOUBLE);

        /** */
        private static final List<SqlBinaryOperator> COMPARISON_OPERATORS =
            ImmutableList.of(
                SqlStdOperatorTable.LESS_THAN,
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                SqlStdOperatorTable.GREATER_THAN,
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);

        /** */
        private static final List<SqlOperator> CHECKED_OPERATORS =
            ImmutableList.of(
                CHECKED_DIVIDE, CHECKED_PLUS, CHECKED_MINUS, CHECKED_MULTIPLY,
                CHECKED_UNARY_MINUS);

        /** */
        private static final List<SqlBinaryOperator> EQUALS_OPERATORS =
            ImmutableList.of(
                SqlStdOperatorTable.EQUALS,
                SqlStdOperatorTable.NOT_EQUALS);

        /** */
        public static final String METHOD_POSTFIX_FOR_ANY_TYPE = "Any";

        /** */
        private final ExpressionType expressionType;

        /** */
        private final String backupMethodName;

        /** */
        BinaryImplementor(
            NullPolicy nullPolicy,
            boolean harmonize,
            ExpressionType expressionType,
            String backupMethodName
        ) {
            super("binary_call", nullPolicy, harmonize);
            this.expressionType = expressionType;
            this.backupMethodName = requireNonNull(backupMethodName, "backupMethodName");
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(
            final RexToLixTranslator translator,
            final RexCall call,
            final List<Expression> argValueList) {
            // neither nullable:
            //   return x OP y
            // x nullable
            //   null_returns_null
            //     return x == null ? null : x OP y
            //   ignore_null
            //     return x == null ? null : y
            // x, y both nullable
            //   null_returns_null
            //     return x == null || y == null ? null : x OP y
            //   ignore_null
            //     return x == null ? y : y == null ? x : x OP y
            // If one or both operands have ANY type, use the late-binding backup
            // method.
            if (anyAnyOperands(call))
                return callBackupMethodAnyType(translator, call, argValueList);

            final Type type0 = argValueList.get(0).getType();
            final Type type1 = argValueList.get(1).getType();
            final SqlBinaryOperator op = (SqlBinaryOperator)call.getOperator();
            final RelDataType relDataType0 = call.getOperands().get(0).getType();
            final Expression fieldComparator = generateCollatorExpression(relDataType0.getCollation());
            if (fieldComparator != null)
                argValueList.add(fieldComparator);

            final Primitive primitive = Primitive.ofBoxOr(type0);
            if (primitive == null
                || type1 == BigDecimal.class
                || COMPARISON_OPERATORS.contains(op)
                && !COMP_OP_TYPES.contains(primitive)) {
                return Expressions.call(SqlFunctions.class, backupMethodName,
                    argValueList);
            }
            // When checking equals or not equals on two primitive boxing classes
            // (i.e. Long x, Long y), we should fall back to call `SqlFunctions.eq(x, y)`
            // or `SqlFunctions.ne(x, y)`, rather than `x == y`
            final Primitive boxPrimitive0 = Primitive.ofBox(type0);
            final Primitive boxPrimitive1 = Primitive.ofBox(type1);
            if (EQUALS_OPERATORS.contains(op)
                && boxPrimitive0 != null && boxPrimitive1 != null) {
                return Expressions.call(SqlFunctions.class, backupMethodName,
                    argValueList);
            }

            // For checked arithmetic call the method.
            if (CHECKED_OPERATORS.contains(op))
                return Expressions.call(SqlFunctions.class, backupMethodName, argValueList);

            return IgniteExpressions.makeBinary(expressionType,
                argValueList.get(0), argValueList.get(1));
        }

        /** Returns whether any of a call's operands have ANY type. */
        private boolean anyAnyOperands(RexCall call) {
            for (RexNode operand : call.operands) {
                if (operand.getType().getSqlTypeName() == SqlTypeName.ANY)
                    return true;
            }
            return false;
        }

        /** */
        private Expression callBackupMethodAnyType(RexToLixTranslator translator,
            RexCall call, List<Expression> expressions) {
            final String backupMethodNameForAnyType =
                backupMethodName + METHOD_POSTFIX_FOR_ANY_TYPE;

            // one or both of parameter(s) is(are) ANY type
            final Expression expression0 = maybeBox(expressions.get(0));
            final Expression expression1 = maybeBox(expressions.get(1));
            return Expressions.call(SqlFunctions.class, backupMethodNameForAnyType,
                expression0, expression1);
        }

        /** */
        private Expression maybeBox(Expression expression) {
            final Primitive primitive = Primitive.of(expression.getType());
            if (primitive != null)
                expression = Expressions.box(expression, primitive);

            return expression;
        }
    }

    /** Implementor for unary operators. */
    private static class UnaryImplementor extends AbstractRexCallImplementor {
        /** */
        private final ExpressionType expressionType;

        /** */
        private final String backupMethodName;

        /** */
        UnaryImplementor(
            ExpressionType expressionType,
            NullPolicy nullPolicy,
            String backupMethodName
        ) {
            super("unary_call", nullPolicy, false);
            this.expressionType = expressionType;
            this.backupMethodName = backupMethodName;
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
            RexCall call, List<Expression> argValueList) {
            final Expression argVal = argValueList.get(0);

            final Expression e;
            //Special case for implementing unary minus with BigDecimal type
            //for other data type(except BigDecimal) '-' operator is OK, but for
            //BigDecimal, we should call negate method of BigDecimal
            if (expressionType == ExpressionType.Negate && argVal.type == BigDecimal.class
                && null != backupMethodName)
                e = Expressions.call(argVal, backupMethodName);
            else
                e = IgniteExpressions.makeUnary(expressionType, argVal);

            if (e.type.equals(argVal.type))
                return e;
            // Certain unary operators do not preserve type. For example, the "-"
            // operator applied to a "byte" expression returns an "int".
            return Expressions.convert_(e, argVal.type);
        }
    }

    /** Implementor for the {@code EXTRACT(unit FROM datetime)} function. */
    private static class ExtractImplementor extends AbstractRexCallImplementor {
        /** */
        ExtractImplementor() {
            super("extract", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final TimeUnitRange timeUnitRange =
                (TimeUnitRange)translator.getLiteralValue(argValueList.get(0));
            final TimeUnit unit = timeUnitRange.startUnit;
            Expression operand = argValueList.get(1);
            final SqlTypeName sqlTypeName =
                call.operands.get(1).getType().getSqlTypeName();
            final boolean isIntervalType = SqlTypeUtil.isInterval(call.operands.get(1).getType());

            switch (unit) {
                case MILLENNIUM:
                case CENTURY:
                case YEAR:
                case QUARTER:
                case MONTH:
                case DAY:
                case DOW:
                case DECADE:
                case DOY:
                case ISODOW:
                case ISOYEAR:
                case WEEK:
                    switch (sqlTypeName) {
                        case INTERVAL_YEAR:
                        case INTERVAL_YEAR_MONTH:
                        case INTERVAL_MONTH:
                        case INTERVAL_DAY:
                        case INTERVAL_DAY_HOUR:
                        case INTERVAL_DAY_MINUTE:
                        case INTERVAL_DAY_SECOND:
                        case INTERVAL_HOUR:
                        case INTERVAL_HOUR_MINUTE:
                        case INTERVAL_HOUR_SECOND:
                        case INTERVAL_MINUTE:
                        case INTERVAL_MINUTE_SECOND:
                        case INTERVAL_SECOND:
                            break;
                        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                            operand = Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
                            // fall through
                        case TIMESTAMP:
                            operand = Expressions.call(BuiltInMethod.FLOOR_DIV.method,
                                operand, Expressions.constant(TimeUnit.DAY.multiplier.longValue()));
                            // fall through
                        case DATE:
                            return Expressions.call(BuiltInMethod.UNIX_DATE_EXTRACT.method,
                                argValueList.get(0), operand);
                        default:
                            throw new AssertionError("unexpected " + sqlTypeName);
                    }
                    break;
                case MILLISECOND:
                case MICROSECOND:
                case NANOSECOND:
                    if (sqlTypeName == SqlTypeName.DATE)
                        return Expressions.constant(0L);

                    operand = mod(operand, TimeUnit.MINUTE.multiplier.longValue(), !isIntervalType);
                    return Expressions.multiply(
                        operand, Expressions.constant((long)(1 / unit.multiplier.doubleValue())));
                case EPOCH:
                    switch (sqlTypeName) {
                        case DATE:
                            // convert to milliseconds
                            operand = Expressions.multiply(operand,
                                Expressions.constant(TimeUnit.DAY.multiplier.longValue()));
                            // fall through
                        case TIMESTAMP:
                            // convert to seconds
                            return Expressions.divide(operand,
                                Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
                        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                            operand = Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
                            return Expressions.divide(operand,
                                Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
                        case INTERVAL_YEAR:
                        case INTERVAL_YEAR_MONTH:
                        case INTERVAL_MONTH:
                        case INTERVAL_DAY:
                        case INTERVAL_DAY_HOUR:
                        case INTERVAL_DAY_MINUTE:
                        case INTERVAL_DAY_SECOND:
                        case INTERVAL_HOUR:
                        case INTERVAL_HOUR_MINUTE:
                        case INTERVAL_HOUR_SECOND:
                        case INTERVAL_MINUTE:
                        case INTERVAL_MINUTE_SECOND:
                        case INTERVAL_SECOND:
                            // no convertlet conversion, pass it as extract
                            throw new AssertionError("unexpected " + sqlTypeName);
                    }
                    break;
                case HOUR:
                case MINUTE:
                case SECOND:
                    switch (sqlTypeName) {
                        case DATE:
                            return Expressions.multiply(operand, Expressions.constant(0L));
                    }
                    break;
            }

            // According to SQL standard result for interval data types should have the same sign as the source,
            // but QUARTER is not covered by standard and negative values for QUARTER make no sense.
            operand = mod(operand, getFactor(unit), unit == TimeUnit.QUARTER || !isIntervalType );

            if (unit == TimeUnit.QUARTER)
                operand = Expressions.subtract(operand, Expressions.constant(1L));

            operand = Expressions.divide(operand,
                Expressions.constant(unit.multiplier.longValue()));

            if (unit == TimeUnit.QUARTER)
                operand = Expressions.add(operand, Expressions.constant(1L));

            return operand;
        }
    }

    /** */
    private static Expression mod(Expression operand, long factor, boolean floorMod) {
        if (factor == 1L)
            return operand;
        else {
            return floorMod ? Expressions.call(BuiltInMethod.FLOOR_MOD.method, operand, Expressions.constant(factor)) :
                Expressions.modulo(operand, Expressions.constant(factor));
        }
    }

    /** */
    private static long getFactor(TimeUnit unit) {
        switch (unit) {
            case DAY:
                return 1L;
            case HOUR:
                return TimeUnit.DAY.multiplier.longValue();
            case MINUTE:
                return TimeUnit.HOUR.multiplier.longValue();
            case SECOND:
                return TimeUnit.MINUTE.multiplier.longValue();
            case MILLISECOND:
                return TimeUnit.SECOND.multiplier.longValue();
            case MONTH:
                return TimeUnit.YEAR.multiplier.longValue();
            case QUARTER:
                return TimeUnit.YEAR.multiplier.longValue();
            case YEAR:
            case DECADE:
            case CENTURY:
            case MILLENNIUM:
                return 1L;
            default:
                throw Util.unexpected(unit);
        }
    }

    /** Implementor for the SQL {@code COALESCE} operator. */
    private static class CoalesceImplementor extends AbstractRexCallImplementor {
        /** */
        CoalesceImplementor() {
            super("coalesce", NullPolicy.NONE, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return implementRecurse(translator, argValueList);
        }

        /** */
        private Expression implementRecurse(RexToLixTranslator translator,
            final List<Expression> argValueList) {
            if (argValueList.size() == 1)
                return argValueList.get(0);
            else {
                return Expressions.condition(
                    translator.checkNotNull(argValueList.get(0)),
                    argValueList.get(0),
                    implementRecurse(translator, Util.skip(argValueList)));
            }
        }
    }

    /** Implementor for the SQL {@code CAST} operator. */
    private static class CastImplementor extends AbstractRexCallImplementor {
        /** */
        CastImplementor() {
            super("cast", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            assert call.getOperands().size() == 1;
            final RelDataType srcType = call.getOperands().get(0).getType();

            // Short-circuit if no cast is required
            RexNode arg = call.getOperands().get(0);
            if (call.getType().equals(srcType)) {
                // No cast required, omit cast
                return argValueList.get(0);
            }
            if (SqlTypeUtil.equalSansNullability(translator.typeFactory,
                call.getType(), arg.getType())
                && translator.deref(arg) instanceof RexLiteral) {
                return RexToLixTranslator.translateLiteral(
                    (RexLiteral)translator.deref(arg), call.getType(),
                    translator.typeFactory, NullAs.NULL);
            }
            final RelDataType targetType =
                nullifyType(translator.typeFactory, call.getType(), false);
            return translator.translateCast(srcType,
                targetType, argValueList.get(0));
        }

        /** */
        private RelDataType nullifyType(JavaTypeFactory typeFactory,
            final RelDataType type, final boolean nullable) {
            if (type instanceof RelDataTypeFactoryImpl.JavaType) {
                final Primitive primitive = Primitive.ofBox(
                    ((RelDataTypeFactoryImpl.JavaType)type).getJavaClass());
                if (primitive != null)
                    return typeFactory.createJavaType(primitive.primitiveClass);
            }
            return typeFactory.createTypeWithNullability(type, nullable);
        }
    }

    /** Implementor for the {@code REINTERPRET} internal SQL operator. */
    private static class ReinterpretImplementor extends AbstractRexCallImplementor {
        /** */
        ReinterpretImplementor() {
            super("reInterpret", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            assert call.getOperands().size() == 1;
            return argValueList.get(0);
        }
    }

    /** Implementor for a value-constructor. */
    private static class ValueConstructorImplementor
        extends AbstractRexCallImplementor {

        /** */
        ValueConstructorImplementor() {
            super("value_constructor", NullPolicy.NONE, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            SqlKind kind = call.getOperator().getKind();
            final BlockBuilder blockBuilder = translator.getBlockBuilder();
            switch (kind) {
                case MAP_VALUE_CONSTRUCTOR:
                    Expression map =
                        blockBuilder.append("map", Expressions.new_(LinkedHashMap.class),
                            false);
                    for (int i = 0; i < argValueList.size(); i++) {
                        Expression key = argValueList.get(i++);
                        Expression val = argValueList.get(i);
                        blockBuilder.add(
                            Expressions.statement(
                                Expressions.call(map, BuiltInMethod.MAP_PUT.method,
                                    Expressions.box(key), Expressions.box(val))));
                    }
                    return map;
                case ARRAY_VALUE_CONSTRUCTOR:
                    Expression lyst =
                        blockBuilder.append("list", Expressions.new_(ArrayList.class),
                            false);
                    for (Expression val : argValueList) {
                        blockBuilder.add(
                            Expressions.statement(
                                Expressions.call(lyst, BuiltInMethod.COLLECTION_ADD.method,
                                    Expressions.box(val))));
                    }
                    return lyst;
                default:
                    throw new AssertionError("unexpected: " + kind);
            }
        }
    }

    /** Implementor for indexing an array using the {@code ITEM} SQL operator
     * and the {@code OFFSET}, {@code ORDINAL}, {@code SAFE_OFFSET}, and
     * {@code SAFE_ORDINAL} BigQuery operators. */
    private static class ArrayItemImplementor extends AbstractRexCallImplementor {
        /** */
        ArrayItemImplementor() {
            super("array_item", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final SqlItemOperator itemOperator = (SqlItemOperator)call.getOperator();
            return Expressions.call(BuiltInMethod.ARRAY_ITEM.method,
                Expressions.list(argValueList)
                    .append(Expressions.constant(itemOperator.offset))
                    .append(Expressions.constant(itemOperator.safe)));
        }
    }

    /** General implementor for indexing a collection using the {@code ITEM} SQL operator. If the
     * collection is an array, an instance of the ArrayItemImplementor is used to handle
     * additional offset and out-of-bounds behavior that is only applicable for arrays. */
    private static class ItemImplementor extends AbstractRexCallImplementor {
        /** */
        ItemImplementor() {
            super("item", NullPolicy.STRICT, false);
        }

        // Since we follow PostgreSQL's semantics that an out-of-bound reference
        // returns NULL, x[y] can return null even if x and y are both NOT NULL.
        // (In SQL standard semantics, an out-of-bound reference to an array
        // throws an exception.)
        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final AbstractRexCallImplementor implementor =
                getImplementor(call.getOperands().get(0).getType().getSqlTypeName());
            return implementor.implementSafe(translator, call, argValueList);
        }

        /** */
        private AbstractRexCallImplementor getImplementor(SqlTypeName sqlTypeName) {
            switch (sqlTypeName) {
                case ARRAY:
                    return new ArrayItemImplementor();
                case MAP:
                    return new MethodImplementor(BuiltInMethod.MAP_ITEM.method, nullPolicy, false);
                default:
                    return new MethodImplementor(BuiltInMethod.ANY_ITEM.method, nullPolicy, false);
            }
        }
    }

    /**
     * Implementor for SQL system functions.
     *
     * <p>Several of these are represented internally as constant values, set
     * per execution.
     */
    private static class SystemFunctionImplementor
        extends AbstractRexCallImplementor {
        /** */
        SystemFunctionImplementor() {
            super("system_func", NullPolicy.NONE, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final SqlOperator op = call.getOperator();
            final Expression root = translator.getRoot();
            if (op == CURRENT_TIMESTAMP)
                return Expressions.call(BuiltInMethod.CURRENT_TIMESTAMP.method, root);
            else if (op == CURRENT_TIME)
                return Expressions.call(BuiltInMethod.CURRENT_TIME.method, root);
            else if (op == CURRENT_DATE)
                return Expressions.call(BuiltInMethod.CURRENT_DATE.method, root);
            else if (op == LOCALTIMESTAMP)
                return Expressions.call(BuiltInMethod.LOCAL_TIMESTAMP.method, root);
            else if (op == LOCALTIME)
                return Expressions.call(BuiltInMethod.LOCAL_TIME.method, root);
            else if (op == SYSTEM_RANGE) {
                if (call.getOperands().size() == 2)
                    return createTableFunctionImplementor(IgniteMethod.SYSTEM_RANGE2.method())
                        .implement(translator, call, NullAs.NULL);

                if (call.getOperands().size() == 3)
                    return createTableFunctionImplementor(IgniteMethod.SYSTEM_RANGE3.method())
                        .implement(translator, call, NullAs.NULL);
            }
            else if (op == TYPEOF) {
                assert call.getOperands().size() == 1 : call.getOperands();

                return typeOfImplementor().implement(translator, call, NullAs.NOT_POSSIBLE);
            }
            else if (op == QUERY_ENGINE)
                return Expressions.constant(CalciteQueryEngineConfiguration.ENGINE_NAME);
            else if (op == NULL_BOUND)
                return Expressions.call(root, IgniteMethod.CONTEXT_NULL_BOUND.method());

            throw new AssertionError("unknown function " + op);
        }
    }

    /** */
    private static CallImplementor typeOfImplementor() {
        return createImplementor((translator, call, translatedOperands) -> {
            Method method = IgniteMethod.SKIP_FIRST_ARGUMENT.method();

            RexNode operand = call.getOperands().get(0);
            String operandType = operand.getType().toString();

            List<Expression> finalOperands = new ArrayList<>(2);
            // The first argument is an arbitrary expression (must be evaluated).
            // The second argument is a type of the first expression (a constant).
            finalOperands.add(translatedOperands.get(0));
            finalOperands.add(Expressions.constant(operandType));

            return Expressions.call(method, finalOperands);
        }, NullPolicy.NONE, false);
    }

    /** Implementor for the {@code NOT} operator. */
    private static class NotImplementor extends AbstractRexCallImplementor {
        /** */
        private AbstractRexCallImplementor implementor;

        /** */
        private NotImplementor(NullPolicy nullPolicy, AbstractRexCallImplementor implementor) {
            super("not", nullPolicy, false);
            this.implementor = implementor;
        }

        /** */
        static AbstractRexCallImplementor of(AbstractRexCallImplementor implementor) {
            return new NotImplementor(implementor.nullPolicy, implementor);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final Expression expression =
                implementor.implementSafe(translator, call, argValueList);
            return Expressions.not(expression);
        }
    }

    /** Implementor for various datetime arithmetic. */
    private static class DatetimeArithmeticImplementor
        extends AbstractRexCallImplementor {
        /** */
        DatetimeArithmeticImplementor() {
            super("dateTime_arithmetic", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            final RexNode operand0 = call.getOperands().get(0);
            Expression trop0 = argValueList.get(0);
            final SqlTypeName typeName1 =
                call.getOperands().get(1).getType().getSqlTypeName();
            Expression trop1 = argValueList.get(1);
            final SqlTypeName typeName = call.getType().getSqlTypeName();
            switch (operand0.getType().getSqlTypeName()) {
                case DATE:
                    switch (typeName) {
                        case TIMESTAMP:
                            trop0 = Expressions.convert_(
                                Expressions.multiply(trop0,
                                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                                long.class);
                            break;
                        default:
                            switch (typeName1) {
                                case INTERVAL_DAY:
                                case INTERVAL_DAY_HOUR:
                                case INTERVAL_DAY_MINUTE:
                                case INTERVAL_DAY_SECOND:
                                case INTERVAL_HOUR:
                                case INTERVAL_HOUR_MINUTE:
                                case INTERVAL_HOUR_SECOND:
                                case INTERVAL_MINUTE:
                                case INTERVAL_MINUTE_SECOND:
                                case INTERVAL_SECOND:
                                    trop1 = IgniteExpressions.convertChecked(
                                        Expressions.divide(trop1,
                                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                                        Primitive.of(long.class), Primitive.of(int.class));
                            }
                    }
                    break;
                case TIME:
                    trop1 = Expressions.convert_(trop1, int.class);
                    break;
            }
            switch (typeName1) {
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    switch (call.getKind()) {
                        case MINUS:
                            trop1 = Expressions.negate(trop1);
                    }
                    switch (typeName) {
                        case TIME:
                            return Expressions.convert_(trop0, long.class);
                        default:
                            final BuiltInMethod method =
                                operand0.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP
                                    ? BuiltInMethod.ADD_MONTHS
                                    : BuiltInMethod.ADD_MONTHS_INT;
                            return Expressions.call(method.method, trop0, trop1);
                    }

                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    switch (call.getKind()) {
                        case MINUS:
                            return normalize(typeName, IgniteExpressions.subtractExact(trop0, trop1));
                        default:
                            return normalize(typeName, IgniteExpressions.addExact(trop0, trop1));
                    }

                default:
                    switch (call.getKind()) {
                        case MINUS:
                            switch (typeName) {
                                case INTERVAL_YEAR:
                                case INTERVAL_YEAR_MONTH:
                                case INTERVAL_MONTH:
                                    return Expressions.call(BuiltInMethod.SUBTRACT_MONTHS.method,
                                        trop0, trop1);
                            }
                            TimeUnit fromUnit =
                                typeName1 == SqlTypeName.DATE ? TimeUnit.DAY : TimeUnit.MILLISECOND;
                            TimeUnit toUnit = TimeUnit.MILLISECOND;
                            return multiplyDivide(
                                Expressions.convert_(Expressions.subtract(trop0, trop1),
                                    (Class)long.class),
                                fromUnit.multiplier, toUnit.multiplier);
                        default:
                            throw new AssertionError(call);
                    }
            }
        }

        /** Normalizes a TIME value into 00:00:00..23:59:39. */
        private Expression normalize(SqlTypeName typeName, Expression e) {
            switch (typeName) {
                case TIME:
                    return Expressions.call(BuiltInMethod.FLOOR_MOD.method, e,
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
                default:
                    return e;
            }
        }
    }

    /** Null-safe implementor of {@code RexCall}s. */
    public interface RexCallImplementor {
        /** */
        RexToLixTranslator.Result implement(
            RexToLixTranslator translator,
            RexCall call,
            List<RexToLixTranslator.Result> arguments);
    }

    /**
     * Abstract implementation of the {@link RexCallImplementor} interface.
     *
     * <p>It is not always safe to execute the {@link RexCall} directly due to
     * the special null arguments. Therefore, the generated code logic is conditional correspondingly.
     *
     * <p>For example, {@code a + b} will generate two declaration statements:
     *
     * <blockquote>
     * <code>
     * final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b);<br> final boolean xxx_isNull = xxx_value ==
     * null;
     * </code>
     * </blockquote>
     */
    private abstract static class AbstractRexCallImplementor
        implements RexCallImplementor {
        /** */
        final NullPolicy nullPolicy;

        /** */
        final String variableName;

        /** */
        private final boolean harmonize;

        /** */
        AbstractRexCallImplementor(String variableName, NullPolicy nullPolicy, boolean harmonize) {
            this.variableName = requireNonNull(variableName, "variableName");
            this.nullPolicy = requireNonNull(nullPolicy, "nullPolicy");
            this.harmonize = harmonize;
        }

        /** {@inheritDoc} */
        @Override public RexToLixTranslator.Result implement(
            final RexToLixTranslator translator,
            final RexCall call,
            final List<RexToLixTranslator.Result> arguments) {
            final List<Expression> argIsNullList = new ArrayList<>();
            final List<Expression> argValList = new ArrayList<>();
            for (RexToLixTranslator.Result result : arguments) {
                argIsNullList.add(result.isNullVariable);
                argValList.add(result.valueVariable);
            }
            final Expression condition = getCondition(argIsNullList);
            final ParameterExpression valVariable =
                genValueStatement(translator, call, argValList, condition);
            final ParameterExpression isNullVariable =
                genIsNullStatement(translator, valVariable);
            return new RexToLixTranslator.Result(isNullVariable, valVariable);
        }

        /** Figures out conditional expression according to NullPolicy. */
        Expression getCondition(final List<Expression> argIsNullList) {
            if (argIsNullList.isEmpty()
                || nullPolicy == null
                || nullPolicy == NullPolicy.NONE)
                return FALSE_EXPR;

            if (nullPolicy == NullPolicy.ARG0)
                return argIsNullList.get(0);

            return Expressions.foldOr(argIsNullList);
        }

        /** */
        // E.g., "final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b)"
        private ParameterExpression genValueStatement(
            final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList,
            final Expression condition) {
            List<Expression> optimizedArgValList = argValueList;
            if (harmonize) {
                optimizedArgValList =
                    harmonize(optimizedArgValList, translator, call);
            }
            optimizedArgValList = unboxIfNecessary(optimizedArgValList);

            final Expression callVal =
                implementSafe(translator, call, optimizedArgValList);

            // In general, RexCall's type is correct for code generation
            // and thus we should ensure the consistency.
            // However, for some special cases (e.g., TableFunction),
            // the implementation's type is correct, we can't convert it.
            final SqlOperator op = call.getOperator();
            final Type returnType = translator.typeFactory.getJavaClass(call.getType());
            final boolean noConvert = (returnType == null)
                || (returnType == callVal.getType())
                || (op instanceof SqlTableFunction)
                || (op instanceof SqlUserDefinedTableMacro)
                || (op instanceof SqlUserDefinedTableFunction);
            final Expression convertedCallVal =
                noConvert
                    ? callVal
                    : ConverterUtils.convert(callVal, call.getType());

            final Expression valExpression =
                Expressions.condition(condition,
                    getIfTrue(convertedCallVal.getType(), argValueList),
                    convertedCallVal);
            final ParameterExpression val =
                Expressions.parameter(convertedCallVal.getType(),
                    translator.getBlockBuilder().newName(variableName + "_value"));
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, val, valExpression));
            return val;
        }

        /** */
        Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return getDefaultValue(type);
        }

        /** */
        // E.g., "final boolean xxx_isNull = xxx_value == null"
        private ParameterExpression genIsNullStatement(
            final RexToLixTranslator translator, final ParameterExpression value) {
            final ParameterExpression isNullVariable =
                Expressions.parameter(Boolean.TYPE,
                    translator.getBlockBuilder().newName(variableName + "_isNull"));
            final Expression isNullExpression = translator.checkNull(value);
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
            return isNullVariable;
        }

        /** Ensures that operands have identical type. */
        private List<Expression> harmonize(final List<Expression> argValueList,
            final RexToLixTranslator translator, final RexCall call) {
            int nullCnt = 0;
            final List<RelDataType> types = new ArrayList<>();
            final RelDataTypeFactory typeFactory =
                translator.builder.getTypeFactory();
            for (RexNode operand : call.getOperands()) {
                RelDataType type = operand.getType();
                type = toSql(typeFactory, type);
                if (translator.isNullable(operand))
                    ++nullCnt;
                else
                    type = typeFactory.createTypeWithNullability(type, false);

                types.add(type);
            }
            if (allSame(types)) {
                // Operands have the same nullability and type. Return them
                // unchanged.
                return argValueList;
            }
            final RelDataType type = typeFactory.leastRestrictive(types);
            if (type == null) {
                // There is no common type. Presumably this is a binary operator with
                // asymmetric arguments (e.g. interval / integer) which is not intended
                // to be harmonized.
                return argValueList;
            }
            assert (nullCnt > 0) == type.isNullable();
            final Type javaCls =
                translator.typeFactory.getJavaClass(type);
            final List<Expression> harmonizedArgValues = new ArrayList<>();
            for (Expression argVal : argValueList) {
                harmonizedArgValues.add(
                    EnumUtils.convert(argVal, javaCls));
            }
            return harmonizedArgValues;
        }

        /**
         * Under null check, it is safe to unbox the operands before entering the implementor.
         */
        private List<Expression> unboxIfNecessary(final List<Expression> argValueList) {
            List<Expression> unboxValList = argValueList;
            if (nullPolicy == NullPolicy.STRICT || nullPolicy == NullPolicy.ANY
                || nullPolicy == NullPolicy.SEMI_STRICT) {
                unboxValList = argValueList.stream()
                    .map(this::unboxExpression)
                    .collect(Collectors.toList());
            }
            if (nullPolicy == NullPolicy.ARG0 && !argValueList.isEmpty()) {
                final Expression unboxArg0 = unboxExpression(unboxValList.get(0));
                unboxValList.set(0, unboxArg0);
            }
            return unboxValList;
        }

        /** */
        private Expression unboxExpression(final Expression argValue) {
            Primitive fromBox = Primitive.ofBox(argValue.getType());
            if (fromBox == null || fromBox == Primitive.VOID)
                return argValue;

            // Optimization: for "long x";
            // "Long.valueOf(x)" generates "x"
            if (argValue instanceof MethodCallExpression) {
                MethodCallExpression mce = (MethodCallExpression)argValue;
                if (mce.method.getName().equals("valueOf") && mce.expressions.size() == 1) {
                    Expression originArg = mce.expressions.get(0);
                    if (Primitive.of(originArg.type) == fromBox)
                        return originArg;
                }
            }
            return NullAs.NOT_POSSIBLE.handle(argValue);
        }

        /** */
        abstract Expression implementSafe(RexToLixTranslator translator,
            RexCall call, List<Expression> argValueList);
    }

    /**
     * Implementor for the {@code AND} operator.
     *
     * <p>If any of the arguments are false, result is false;
     * else if any arguments are null, result is null; else true.
     */
    private static class LogicalAndImplementor extends AbstractRexCallImplementor {
        /** */
        LogicalAndImplementor() {
            super("logical_and", NullPolicy.NONE, true);
        }

        /** {@inheritDoc} */
        @Override public RexToLixTranslator.Result implement(final RexToLixTranslator translator,
            final RexCall call, final List<RexToLixTranslator.Result> arguments) {
            final List<Expression> argIsNullList = new ArrayList<>();
            for (RexToLixTranslator.Result result : arguments)
                argIsNullList.add(result.isNullVariable);

            final List<Expression> nullAsTrue =
                arguments.stream()
                    .map(result ->
                        Expressions.condition(result.isNullVariable, TRUE_EXPR,
                            result.valueVariable))
                    .collect(Collectors.toList());
            final Expression hasFalse =
                Expressions.not(Expressions.foldAnd(nullAsTrue));
            final Expression hasNull = Expressions.foldOr(argIsNullList);
            final Expression callExpression =
                Expressions.condition(hasFalse, BOXED_FALSE_EXPR,
                    Expressions.condition(hasNull, NULL_EXPR, BOXED_TRUE_EXPR));
            final RexImpTable.NullAs nullAs = translator.isNullable(call)
                ? RexImpTable.NullAs.NULL : RexImpTable.NullAs.NOT_POSSIBLE;
            final Expression valExpression = nullAs.handle(callExpression);
            final ParameterExpression valVariable =
                Expressions.parameter(valExpression.getType(),
                    translator.getBlockBuilder().newName(variableName + "_value"));
            final Expression isNullExpression = translator.checkNull(valVariable);
            final ParameterExpression isNullVariable =
                Expressions.parameter(Boolean.TYPE,
                    translator.getBlockBuilder().newName(variableName + "_isNull"));
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, valVariable, valExpression));
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
            return new RexToLixTranslator.Result(isNullVariable, valVariable);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return null;
        }
    }

    /**
     * Implementor for the {@code OR} operator.
     *
     * <p>If any of the arguments are true, result is true;
     * else if any arguments are null, result is null; else false.
     */
    private static class LogicalOrImplementor extends AbstractRexCallImplementor {
        /** */
        LogicalOrImplementor() {
            super("logical_or", NullPolicy.NONE, true);
        }

        /** {@inheritDoc} */
        @Override public RexToLixTranslator.Result implement(final RexToLixTranslator translator,
            final RexCall call, final List<RexToLixTranslator.Result> arguments) {
            final List<Expression> argIsNullList = new ArrayList<>();
            for (RexToLixTranslator.Result result : arguments)
                argIsNullList.add(result.isNullVariable);

            final List<Expression> nullAsFalse =
                arguments.stream()
                    .map(result ->
                        Expressions.condition(result.isNullVariable, FALSE_EXPR,
                            result.valueVariable))
                    .collect(Collectors.toList());
            final Expression hasTrue = Expressions.foldOr(nullAsFalse);
            final Expression hasNull = Expressions.foldOr(argIsNullList);
            final Expression callExpression =
                Expressions.condition(hasTrue, BOXED_TRUE_EXPR,
                    Expressions.condition(hasNull, NULL_EXPR, BOXED_FALSE_EXPR));
            final RexImpTable.NullAs nullAs = translator.isNullable(call)
                ? RexImpTable.NullAs.NULL : RexImpTable.NullAs.NOT_POSSIBLE;
            final Expression valExpression = nullAs.handle(callExpression);
            final ParameterExpression valVariable =
                Expressions.parameter(valExpression.getType(),
                    translator.getBlockBuilder().newName(variableName + "_value"));
            final Expression isNullExpression = translator.checkNull(valExpression);
            final ParameterExpression isNullVariable =
                Expressions.parameter(Boolean.TYPE,
                    translator.getBlockBuilder().newName(variableName + "_isNull"));
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, valVariable, valExpression));
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
            return new RexToLixTranslator.Result(isNullVariable, valVariable);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return null;
        }
    }

    /**
     * Implementor for the {@code NOT} operator.
     *
     * <p>If any of the arguments are false, result is true;
     * else if any arguments are null, result is null; else false.
     */
    private static class LogicalNotImplementor extends AbstractRexCallImplementor {
        /** */
        LogicalNotImplementor() {
            super("logical_not", NullPolicy.NONE, true);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.call(BuiltInMethod.NOT.method, argValueList);
        }
    }

    /** Implementor for the {@code LN}, {@code LOG}, and {@code LOG10} operators.
     *
     * <p>Handles all logarithm functions using log rules to determine the
     * appropriate base (i.e. base e for LN).
     */
    private static class LogImplementor extends AbstractRexCallImplementor {
        /** */
        LogImplementor() {
            super("log", NullPolicy.STRICT, true);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.call(BuiltInMethod.LOG.method, args(call, argValueList));
        }

        /** */
        private static List<Expression> args(RexCall call,
            List<Expression> argValueList) {
            Expression operand0 = argValueList.get(0);
            final Expressions.FluentList<Expression> list = Expressions.list(operand0);
            switch (call.getOperator().getName()) {
                case "LOG":
                    if (argValueList.size() == 2) {
                        list.append(argValueList.get(1));
                        break;
                    }
                // fall through
                case "LN":
                    list.append(Expressions.constant(Math.exp(1)));
                    break;
                case "LOG10":
                    list.append(Expressions.constant(BigDecimal.TEN));
                    break;
                default:
                    throw new AssertionError("Operator not found: " + call.getOperator());
            }

            list.append(Expressions.constant(false));

            return list;
        }
    }

    /**
     * Implementation that a {@link java.lang.reflect.Method}.
     *
     * <p>If there are several methods in the list, calls the first that has the
     * right number of arguments.
     *
     * <p>When method is not static, a new instance of the required class is
     * created.
     */
    private static class ReflectiveImplementor extends AbstractRexCallImplementor {
        /** */
        protected final ImmutableList<? extends Method> methods;

        /** */
        ReflectiveImplementor(List<? extends Method> methods) {
            super("reflective_" + methods.get(0).getName(), NullPolicy.STRICT, false);
            this.methods = ImmutableList.copyOf(methods);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator,
            RexCall call, List<Expression> argValList) {
            for (Method method : methods) {
                if (method.getParameterCount() == argValList.size())
                    return implementSafe(method, argValList);
            }
            throw new IllegalArgumentException("no matching method");
        }

        /** */
        protected MethodCallExpression implementSafe(Method method,
            List<Expression> argValList) {
            List<Expression> argValList0 =
                ConverterUtils.fromInternal(method.getParameterTypes(),
                    argValList);
            if (isStatic(method))
                return Expressions.call(method, argValList0);
            else {
                // The class must have a public zero-args constructor.
                final Expression target =
                    Expressions.new_(method.getDeclaringClass());
                return Expressions.call(target, method, argValList0);
            }
        }
    }

    /** Implementor for the {@code PI} operator. */
    private static class PiImplementor extends AbstractRexCallImplementor {
        /** */
        PiImplementor() {
            super("pi", NullPolicy.NONE, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.constant(Math.PI);
        }
    }

    /** Implementor for the {@code IS FALSE} SQL operator. */
    private static class IsFalseImplementor extends AbstractRexCallImplementor {
        /** */
        IsFalseImplementor() {
            super("is_false", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return Expressions.constant(false, type);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.equal(argValueList.get(0), FALSE_EXPR);
        }
    }

    /** Implementor for the {@code IS NOT FALSE} SQL operator. */
    private static class IsNotFalseImplementor extends AbstractRexCallImplementor {
        /** */
        IsNotFalseImplementor() {
            super("is_not_false", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return Expressions.constant(true, type);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.notEqual(argValueList.get(0), FALSE_EXPR);
        }
    }

    /** Implementor for the {@code IS NOT NULL} SQL operator. */
    private static class IsNotNullImplementor extends AbstractRexCallImplementor {
        /** */
        IsNotNullImplementor() {
            super("is_not_null", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return Expressions.constant(false, type);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.notEqual(argValueList.get(0), NULL_EXPR);
        }
    }

    /** Implementor for the {@code IS NOT TRUE} SQL operator. */
    private static class IsNotTrueImplementor extends AbstractRexCallImplementor {
        /** */
        IsNotTrueImplementor() {
            super("is_not_true", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return Expressions.constant(true, type);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.notEqual(argValueList.get(0), TRUE_EXPR);
        }
    }

    /** Implementor for the {@code IS NULL} SQL operator. */
    private static class IsNullImplementor extends AbstractRexCallImplementor {
        /** */
        IsNullImplementor() {
            super("is_null", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return Expressions.constant(true, type);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.equal(argValueList.get(0), NULL_EXPR);
        }
    }

    /** Implementor for the {@code IS TRUE} SQL operator. */
    private static class IsTrueImplementor extends AbstractRexCallImplementor {
        /** */
        IsTrueImplementor() {
            super("is_true", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
            return Expressions.constant(false, type);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.equal(argValueList.get(0), TRUE_EXPR);
        }
    }

    /** Implementor for the {@code DEFAULT} function. */
    private static class DefaultImplementor extends AbstractRexCallImplementor {
        /** */
        DefaultImplementor() {
            super("default", NullPolicy.NONE, true);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(final RexToLixTranslator translator,
            final RexCall call, final List<Expression> argValueList) {
            return Expressions.constant(null);
        }
    }

    /**  */
    private static class ReplaceImplementor extends AbstractRexCallImplementor {
        /** */
        private ReplaceImplementor() {
            super("replace", NullPolicy.STRICT, false);
        }

        /** {@inheritDoc} */
        @Override Expression implementSafe(RexToLixTranslator translator, RexCall call, List<Expression> args) {
            return Expressions.call(BuiltInMethod.REPLACE.method, args.get(0), args.get(1), args.get(2),
                Expressions.constant(true, boolean.class));
        }
    }

    /** */
    private static CallImplementor createTableFunctionImplementor(final Method method) {
        return createImplementor(
            new ReflectiveCallNotNullImplementor(method) {
                @Override public Expression implement(RexToLixTranslator translator,
                    RexCall call, List<Expression> translatedOperands) {
                    Expression expr = super.implement(translator, call,
                        translatedOperands);
                    final Class<?> returnType = method.getReturnType();
                    if (QueryableTable.class.isAssignableFrom(returnType)) {
                        Expression queryable = Expressions.call(
                            Expressions.convert_(expr, QueryableTable.class),
                            BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method,
                            Expressions.call(translator.getRoot(),
                                BuiltInMethod.DATA_CONTEXT_GET_QUERY_PROVIDER.method),
                            Expressions.constant(null, SchemaPlus.class),
                            Expressions.constant(call.getOperator().getName(), String.class));
                        expr = Expressions.call(queryable,
                            BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
                    }
                    else {
                        expr = Expressions.call(expr,
                            BuiltInMethod.SCANNABLE_TABLE_SCAN.method,
                            translator.getRoot());
                    }
                    return expr;
                }
            }, NullPolicy.NONE, false);
    }

    /** */
    public static CallImplementor createImplementor(
        final NotNullImplementor implementor,
        final NullPolicy nullPolicy,
        final boolean harmonize) {
        return (translator, call, nullAs) -> {
            final RexImpTable.RexCallImplementor rexCallImplementor
                = createRexCallImplementor(implementor, nullPolicy, harmonize);
            final List<RexToLixTranslator.Result> args = translator.getCallOperandResult(call);
            assert args != null;
            final RexToLixTranslator.Result result = rexCallImplementor.implement(translator, call, args);
            return nullAs.handle(result.valueVariable);
        };
    }

    /** */
    public static RexCallImplementor createRexCallImplementor(
        final NotNullImplementor implementor,
        final NullPolicy nullPolicy,
        final boolean harmonize) {
        return new AbstractRexCallImplementor("not_null_udf", nullPolicy, harmonize) {
            @Override Expression implementSafe(RexToLixTranslator translator,
                RexCall call, List<Expression> argValueList) {
                return implementor.implement(translator, call, argValueList);
            }
        };
    }
}
