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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH;

/**
 * Translates {@link RexNode REX expressions} to {@link Expression linq4j expressions}.
 */
public class RexToLixTranslator implements RexVisitor<RexToLixTranslator.Result> {
    /** */
    final JavaTypeFactory typeFactory;

    /** */
    final RexBuilder builder;

    /** */
    private final RexProgram program;

    /** */
    final SqlConformance conformance;

    /** */
    private final Expression root;

    /** */
    final RexToLixTranslator.InputGetter inputGetter;

    /** */
    private final BlockBuilder list;

    /** */
    private final Function1<String, InputGetter> correlates;

    /**
     * Map from RexLiteral's variable name to its literal, which is often a ({@link ConstantExpression})) It is used in
     * the some {@code RexCall}'s implementors, such as {@code ExtractImplementor}.
     *
     * @see #getLiteral
     * @see #getLiteralValue
     */
    private final Map<Expression, Expression> literalMap = new HashMap<>();

    /**
     * For {@code RexCall}, keep the list of its operand's {@code Result}. It is useful when creating a {@code
     * CallImplementor}.
     */
    private final Map<RexCall, List<Result>> callOperandResultMap = new HashMap<>();

    /**
     * Map from RexNode under specific storage type to its Result, to avoid generating duplicate code. For {@code
     * RexInputRef}, {@code RexDynamicParam} and {@code RexFieldAccess}.
     */
    private final Map<Pair<RexNode, Type>, Result> rexWithStorageTypeResultMap = new HashMap<>();

    /**
     * Map from RexNode to its Result, to avoid generating duplicate code. For {@code RexLiteral} and {@code RexCall}.
     */
    private final Map<RexNode, Result> rexResultMap = new HashMap<>();

    /** */
    private Type currentStorageType;

    /** */
    private RexToLixTranslator(RexProgram program,
        JavaTypeFactory typeFactory,
        Expression root,
        InputGetter inputGetter,
        BlockBuilder list,
        RexBuilder builder,
        SqlConformance conformance,
        Function1<String, InputGetter> correlates) {
        this.program = program; // may be null
        this.typeFactory = Objects.requireNonNull(typeFactory);
        this.conformance = Objects.requireNonNull(conformance);
        this.root = Objects.requireNonNull(root);
        this.inputGetter = inputGetter;
        this.list = Objects.requireNonNull(list);
        this.builder = Objects.requireNonNull(builder);
        this.correlates = correlates; // may be null
    }

    /**
     * Translates a {@link RexProgram} to a sequence of expressions and declarations.
     *
     * @param program Program to be translated
     * @param typeFactory Type factory
     * @param conformance SQL conformance
     * @param list List of statements, populated with declarations
     * @param outputPhysType Output type, or null
     * @param root Root expression
     * @param inputGetter Generates expressions for inputs
     * @param correlates Provider of references to the values of correlated variables
     * @return Sequence of expressions, optional condition
     */
    public static List<Expression> translateProjects(RexProgram program,
        JavaTypeFactory typeFactory, SqlConformance conformance,
        BlockBuilder list, PhysType outputPhysType, Expression root,
        InputGetter inputGetter, Function1<String, InputGetter> correlates) {
        List<Type> storageTypes = null;
        if (outputPhysType != null) {
            final RelDataType rowType = outputPhysType.getRowType();
            storageTypes = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++)
                storageTypes.add(outputPhysType.getJavaFieldType(i));
        }
        return new RexToLixTranslator(program, typeFactory, root, inputGetter,
            list, new IgniteRexBuilder(typeFactory), conformance, null)
            .setCorrelates(correlates)
            .translateList(program.getProjectList(), storageTypes);
    }

    /** */
    Expression translate(RexNode expr) {
        final RexImpTable.NullAs nullAs =
            RexImpTable.NullAs.of(isNullable(expr));
        return translate(expr, nullAs);
    }

    /** */
    Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
        return translate(expr, nullAs, null);
    }

    /** */
    Expression translate(RexNode expr, Type storageType) {
        final RexImpTable.NullAs nullAs =
            RexImpTable.NullAs.of(isNullable(expr));
        return translate(expr, nullAs, storageType);
    }

    /** */
    Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
        Type storageType) {
        currentStorageType = storageType;
        final Result result = expr.accept(this);
        final Expression translated =
            ConverterUtils.toInternal(result.valueVariable, storageType);
        assert translated != null;
        // When we asked for not null input that would be stored as box, avoid unboxing
        if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs
            && translated.type.equals(storageType))
            return translated;

        return nullAs.handle(translated);
    }

    /** */
    Expression translateCast(
        RelDataType sourceType,
        RelDataType targetType,
        Expression operand) {
        Expression convert = null;
        switch (targetType.getSqlTypeName()) {
            case ANY:
                convert = operand;
                break;
            case DATE:
                switch (sourceType.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        convert =
                            Expressions.call(BuiltInMethod.STRING_TO_DATE.method, operand);
                        break;
                    case TIMESTAMP:
                        convert = Expressions.convert_(
                            Expressions.call(BuiltInMethod.FLOOR_DIV.method,
                                operand, Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                            int.class);
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                }
                break;
            case TIME:
                switch (sourceType.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        convert =
                            Expressions.call(BuiltInMethod.STRING_TO_TIME.method, operand);
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                        break;
                    case TIMESTAMP:
                        convert = Expressions.convert_(
                            Expressions.call(
                                BuiltInMethod.FLOOR_MOD.method,
                                operand,
                                Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                            int.class);
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                }
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                switch (sourceType.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        convert =
                            Expressions.call(BuiltInMethod.STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method, operand);
                        break;
                    case TIME:
                        convert = Expressions.call(
                            BuiltInMethod.TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                            RexImpTable.optimize2(
                                operand,
                                Expressions.call(
                                    BuiltInMethod.UNIX_TIME_TO_STRING.method,
                                    operand)),
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
                        break;
                    case TIMESTAMP:
                        convert = Expressions.call(
                            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                            RexImpTable.optimize2(
                                operand,
                                Expressions.call(
                                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                    operand)),
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                                operand));
                }
                break;
            case TIMESTAMP:
                switch (sourceType.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        convert =
                            Expressions.call(BuiltInMethod.STRING_TO_TIMESTAMP.method, operand);
                        break;
                    case DATE:
                        convert = Expressions.multiply(
                            Expressions.convert_(operand, long.class),
                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
                        break;
                    case TIME:
                        convert =
                            Expressions.add(
                                Expressions.multiply(
                                    Expressions.convert_(
                                        Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                                        long.class),
                                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                                Expressions.convert_(operand, long.class));
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                                Expressions.call(
                                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                switch (sourceType.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        convert =
                            Expressions.call(
                                BuiltInMethod.STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                                operand);
                        break;
                    case DATE:
                        convert = Expressions.call(
                            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                            RexImpTable.optimize2(
                                operand,
                                Expressions.call(
                                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                    Expressions.multiply(
                                        Expressions.convert_(operand, long.class),
                                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)))),
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
                        break;
                    case TIME:
                        convert = Expressions.call(
                            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                            RexImpTable.optimize2(
                                operand,
                                Expressions.call(
                                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                    Expressions.add(
                                        Expressions.multiply(
                                            Expressions.convert_(
                                                Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                                                long.class),
                                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                                        Expressions.convert_(operand, long.class)))),
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                                Expressions.call(
                                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                                operand));
                        break;
                    case TIMESTAMP:
                        convert = Expressions.call(
                            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                            RexImpTable.optimize2(
                                operand,
                                Expressions.call(
                                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                    operand)),
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
                }
                break;
            case BOOLEAN:
                switch (sourceType.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        convert = Expressions.call(
                            BuiltInMethod.STRING_TO_BOOLEAN.method,
                            operand);
                }
                break;
            case CHAR:
            case VARCHAR:
                final SqlIntervalQualifier interval =
                    sourceType.getIntervalQualifier();
                switch (sourceType.getSqlTypeName()) {
                    case DATE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.UNIX_DATE_TO_STRING.method,
                                operand));
                        break;
                    case TIME:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.UNIX_TIME_TO_STRING.method,
                                operand));
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                        break;
                    case TIMESTAMP:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                operand));
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
                        break;
                    case INTERVAL_YEAR:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_MONTH:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                                operand,
                                Expressions.constant(interval.timeUnitRange)));
                        break;
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
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method,
                                operand,
                                Expressions.constant(interval.timeUnitRange),
                                Expressions.constant(
                                    interval.getFractionalSecondPrecision(
                                        typeFactory.getTypeSystem()))));
                        break;
                    case BOOLEAN:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                BuiltInMethod.BOOLEAN_TO_STRING.method,
                                operand));
                        break;
                    case BINARY:
                    case VARBINARY:
                        convert = RexImpTable.optimize2(
                            operand,
                            Expressions.call(IgniteMethod.BYTESTRING_TO_STRING.method(), operand));
                        break;
                }
                break;
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
                switch (sourceType.getSqlTypeName().getFamily()) {
                    case CHARACTER:
                        SqlIntervalQualifier intervalQualifier = targetType.getIntervalQualifier();

                        Method method = intervalQualifier.isYearMonth() ?
                            IgniteMethod.PARSE_INTERVAL_YEAR_MONTH.method() :
                            IgniteMethod.PARSE_INTERVAL_DAY_TIME.method();

                        convert = Expressions.call(
                            method,
                            operand,
                            Expressions.new_(SqlIntervalQualifier.class,
                                Expressions.constant(intervalQualifier.getStartUnit()),
                                Expressions.constant(intervalQualifier.getStartPrecisionPreservingDefault()),
                                Expressions.constant(intervalQualifier.getEndUnit()),
                                Expressions.constant(intervalQualifier.getFractionalSecondPrecisionPreservingDefault()),
                                Expressions.field(null, SqlParserPos.class, "ZERO")
                            )
                        );
                        break;
                    case NUMERIC:
                        BigDecimal multiplier = targetType.getSqlTypeName().getEndUnit().multiplier;

                        if (SqlTypeName.FRACTIONAL_TYPES.contains(sourceType.getSqlTypeName())) {
                            convert = sourceType.getSqlTypeName() == SqlTypeName.DECIMAL
                                ? operand
                                : ConverterUtils.convertToDecimal(operand, typeFactory.createSqlType(SqlTypeName.DECIMAL));

                            convert = Expressions.call(
                                convert,
                                IgniteMethod.BIG_DECIMAL_MULTIPLY.method(),
                                Expressions.constant(multiplier));
                        }
                        else
                            convert = IgniteExpressions.multiplyExact(operand, Expressions.constant(multiplier.longValue()));

                        convert = ConverterUtils.convert(convert, targetType);
                        break;
                }
                break;
            case BINARY:
            case VARBINARY:
                switch (sourceType.getSqlTypeName().getFamily()) {
                    case CHARACTER:
                        convert = Expressions.call(IgniteMethod.STRING_TO_BYTESTRING.method(), operand);
                }
                break;
        }

        if (convert == null)
            convert = ConverterUtils.convert(operand, targetType);

        // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
        // longer than n.
        boolean pad = false;
        boolean truncate = true;
        switch (targetType.getSqlTypeName()) {
            case CHAR:
            case BINARY:
                pad = true;
                // fall through
            case VARCHAR:
            case VARBINARY:
                final int targetPrecision = targetType.getPrecision();
                if (targetPrecision >= 0) {
                    switch (sourceType.getSqlTypeName()) {
                        case CHAR:
                        case VARCHAR:
                        case BINARY:
                        case VARBINARY:
                            // If this is a widening cast, no need to truncate.
                            final int srcPrecision = sourceType.getPrecision();
                            if (SqlTypeUtil.comparePrecision(srcPrecision, targetPrecision) <= 0)
                                truncate = false;

                            // If this is a widening cast, no need to pad.
                            if (SqlTypeUtil.comparePrecision(srcPrecision, targetPrecision) >= 0)
                                pad = false;

                            // fall through
                        default:
                            if (truncate || pad) {
                                convert =
                                    Expressions.call(
                                        pad
                                            ? BuiltInMethod.TRUNCATE_OR_PAD.method
                                            : BuiltInMethod.TRUNCATE.method,
                                        convert,
                                        Expressions.constant(targetPrecision));
                            }
                    }
                }
                break;
            case TIMESTAMP:
                int targetScale = targetType.getScale();
                if (targetScale == RelDataType.SCALE_NOT_SPECIFIED)
                    targetScale = 0;

                if (targetScale < sourceType.getScale()) {
                    convert =
                        Expressions.call(
                            BuiltInMethod.ROUND_LONG.method,
                            convert,
                            Expressions.constant(
                                (long)Math.pow(10, 3 - targetScale)));
                }
                break;
        }
        return scaleIntervalToNumber(sourceType, targetType, convert);
    }

    /**
     * Dereferences an expression if it is a {@link RexLocalRef}.
     */
    public RexNode deref(RexNode expr) {
        if (expr instanceof RexLocalRef) {
            RexLocalRef ref = (RexLocalRef)expr;
            final RexNode e2 = program.getExprList().get(ref.getIndex());
            assert ref.getType().equals(e2.getType());
            return e2;
        }

        return expr;
    }

    /**
     * Translates a literal.
     *
     * @throws ControlFlowException if literal is null but {@code nullAs} is {@link RexImpTable.NullAs#NOT_POSSIBLE}.
     */
    public static Expression translateLiteral(
        RexLiteral literal,
        RelDataType type,
        JavaTypeFactory typeFactory,
        RexImpTable.NullAs nullAs) {
        if (literal.isNull()) {
            switch (nullAs) {
                case TRUE:
                case IS_NULL:
                    return RexImpTable.TRUE_EXPR;
                case FALSE:
                case IS_NOT_NULL:
                    return RexImpTable.FALSE_EXPR;
                case NOT_POSSIBLE:
                    throw new ControlFlowException();
                case NULL:
                default:
                    return RexImpTable.NULL_EXPR;
            }
        }
        else {
            switch (nullAs) {
                case IS_NOT_NULL:
                    return RexImpTable.TRUE_EXPR;
                case IS_NULL:
                    return RexImpTable.FALSE_EXPR;
            }
        }
        Type javaCls = typeFactory.getJavaClass(type);
        final Object value2;
        switch (literal.getType().getSqlTypeName()) {
            case DECIMAL:
                final BigDecimal bd = literal.getValueAs(BigDecimal.class);
                if (javaCls == float.class)
                    return Expressions.constant(bd, javaCls);
                else if (javaCls == double.class)
                    return Expressions.constant(bd, javaCls);
                assert javaCls == BigDecimal.class;
                return Expressions.call(
                    IgniteSqlFunctions.class,
                    "toBigDecimal",
                    /*
                    The ConstantExpression class, when converting from BigDecimal to Bigdecimal,
                    removes trailing zeros from the original object, regardless of the original scale value.
                    Therefore, BigDecimal must be converted to a string to avoid this.
                     */
                    Expressions.constant(bd.toString()),
                    Expressions.constant(type.getPrecision()),
                    Expressions.constant(type.getScale()));
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                value2 = literal.getValueAs(Integer.class);
                javaCls = int.class;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
                value2 = literal.getValueAs(Long.class);
                javaCls = long.class;
                break;
            case CHAR:
            case VARCHAR:
                value2 = literal.getValueAs(String.class);
                break;
            case BINARY:
            case VARBINARY:
                return Expressions.new_(
                    ByteString.class,
                    Expressions.constant(
                        literal.getValueAs(byte[].class),
                        byte[].class));
            case GEOMETRY:
                throw new IllegalStateException("Unsupported data type: " + literal.getType());
            case SYMBOL:
                value2 = literal.getValueAs(Enum.class);
                javaCls = value2.getClass();
                break;
            default:
                final Primitive primitive = Primitive.ofBoxOr(javaCls);
                final Comparable val = literal.getValueAs(Comparable.class);

                value2 = primitive != null && val instanceof Number ? primitive.number((Number)val) : val;
        }
        return Expressions.constant(value2, javaCls);
    }

    /** */
    public List<Expression> translateList(
        List<RexNode> operandList,
        RexImpTable.NullAs nullAs) {
        return translateList(operandList, nullAs,
            ConverterUtils.internalTypes(operandList));
    }

    /** */
    public List<Expression> translateList(
        List<RexNode> operandList,
        RexImpTable.NullAs nullAs,
        List<? extends Type> storageTypes) {
        final List<Expression> list = new ArrayList<>();
        for (Pair<RexNode, ? extends Type> e : Pair.zip(operandList, storageTypes))
            list.add(translate(e.left, nullAs, e.right));

        return list;
    }

    /**
     * Translates the list of {@code RexNode}, using the default output types. This might be suboptimal in terms of
     * additional box-unbox when you use the translation later. If you know the java class that will be used to store
     * the results, use {@link RexToLixTranslator#translateList(List, List)} version.
     *
     * @param operandList list of RexNodes to translate
     * @return translated expressions
     */
    public List<Expression> translateList(List<? extends RexNode> operandList) {
        return translateList(operandList, ConverterUtils.internalTypes(operandList));
    }

    /**
     * Translates the list of {@code RexNode}, while optimizing for output storage. For instance, if the result of
     * translation is going to be stored in {@code Object[]}, and the input is {@code Object[]} as well, then translator
     * will avoid casting, boxing, etc.
     *
     * @param operandList list of RexNodes to translate
     * @param storageTypes hints of the java classes that will be used to store translation results. Use null to use
     * default storage type
     * @return translated expressions
     */
    public List<Expression> translateList(List<? extends RexNode> operandList,
        List<? extends Type> storageTypes) {
        final List<Expression> list = new ArrayList<>(operandList.size());

        for (int i = 0; i < operandList.size(); i++) {
            RexNode rex = operandList.get(i);
            Type desiredType = null;
            if (storageTypes != null)
                desiredType = storageTypes.get(i);

            final Expression translate = translate(rex, desiredType);
            list.add(translate);
            // desiredType is still a hint, thus we might get any kind of output
            // (boxed or not) when hint was provided.
            // It is favourable to get the type matching desired type
            if (desiredType == null && !isNullable(rex)) {
                assert !Primitive.isBox(translate.getType())
                    : "Not-null boxed primitive should come back as primitive: "
                    + rex + ", " + translate.getType();
            }
        }
        return list;
    }

    /**
     * Returns whether an expression is nullable.
     *
     * @param e Expression
     * @return Whether expression is nullable
     */
    public boolean isNullable(RexNode e) {
        return e.getType().isNullable();
    }

    /** */
    public RexToLixTranslator setBlock(BlockBuilder block) {
        if (block == list)
            return this;

        return new RexToLixTranslator(program, typeFactory, root, inputGetter,
            block, builder, conformance, correlates);
    }

    /** */
    public RexToLixTranslator setCorrelates(
        Function1<String, InputGetter> correlates) {
        if (this.correlates == correlates)
            return this;

        return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            builder, conformance, correlates);
    }

    /** */
    public Expression getRoot() {
        return root;
    }

    /** */
    private static Expression scaleIntervalToNumber(
        RelDataType sourceType,
        RelDataType targetType,
        Expression operand) {
        switch (targetType.getSqlTypeName().getFamily()) {
            case NUMERIC:
                switch (sourceType.getSqlTypeName()) {
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
                        // Scale to the given field.
                        final BigDecimal multiplier = BigDecimal.ONE;
                        final BigDecimal divider =
                            sourceType.getSqlTypeName().getEndUnit().multiplier;
                        return RexImpTable.multiplyDivide(operand, multiplier, divider);
                }
        }
        return operand;
    }

    /**
     * Visit {@code RexInputRef}. If it has never been visited under current storage type before, {@code
     * RexToLixTranslator} generally produces three lines of code. For example, when visiting a column (named
     * commission) in table Employee, the generated code snippet is: {@code final Employee current =(Employee)
     * inputEnumerator.current(); final Integer input_value = current.commission; final boolean input_isNull =
     * input_value == null; }
     */
    @Override public Result visitInputRef(RexInputRef inputRef) {
        final Pair<RexNode, Type> key = Pair.of(inputRef, currentStorageType);
        // If the RexInputRef has been visited under current storage type already,
        // it is not necessary to visit it again, just return the result.
        if (rexWithStorageTypeResultMap.containsKey(key))
            return rexWithStorageTypeResultMap.get(key);

        // Generate one line of code to get the input, e.g.,
        // "final Employee current =(Employee) inputEnumerator.current();"
        final Expression valExpression = inputGetter.field(
            list, inputRef.getIndex(), currentStorageType);

        // Generate one line of code for the value of RexInputRef, e.g.,
        // "final Integer input_value = current.commission;"
        final ParameterExpression valVariable =
            Expressions.parameter(
                valExpression.getType(), list.newName("input_value"));
        list.add(Expressions.declare(Modifier.FINAL, valVariable, valExpression));

        // Generate one line of code to check whether RexInputRef is null, e.g.,
        // "final boolean input_isNull = input_value == null;"
        final Expression isNullExpression = checkNull(valVariable);
        final ParameterExpression isNullVariable =
            Expressions.parameter(
                Boolean.TYPE, list.newName("input_isNull"));
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

        final Result result = new Result(isNullVariable, valVariable);

        // Cache <RexInputRef, currentStorageType>'s result
        rexWithStorageTypeResultMap.put(key, result);

        return new Result(isNullVariable, valVariable);
    }

    /** {@inheritDoc} */
    @Override public Result visitLocalRef(RexLocalRef localRef) {
        return deref(localRef).accept(this);
    }

    /** {@inheritDoc} */
    @Override public Result visitLambdaRef(RexLambdaRef ref) {
        final ParameterExpression valVariable =
            Expressions.parameter(
                typeFactory.getJavaClass(ref.getType()), ref.getName());

        // Generate one line of code to check whether lambdaRef is null, e.g.,
        // "final boolean input_isNull = $0 == null;"
        final Expression isNullExpression = checkNull(valVariable);
        final ParameterExpression isNullVariable =
            Expressions.parameter(
                Boolean.TYPE, list.newName("input_isNull"));
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
        return new Result(isNullVariable, valVariable);
    }

    /**
     * Visit {@code RexLiteral}. If it has never been visited before, {@code RexToLixTranslator} will generate two lines
     * of code. For example, when visiting a primitive int (10), the generated code snippet is: {@code final int
     * literal_value = 10; final boolean literal_isNull = false; }
     */
    @Override public Result visitLiteral(RexLiteral literal) {
        // If the RexLiteral has been visited already, just return the result
        if (rexResultMap.containsKey(literal))
            return rexResultMap.get(literal);

        // Generate one line of code for the value of RexLiteral, e.g.,
        // "final int literal_value = 10;"
        final Expression valExpression = literal.isNull()
            // Note: even for null literal, we can't loss its type information
            ? getTypedNullLiteral(literal)
            : translateLiteral(literal, literal.getType(),
            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE);
        final ParameterExpression valVariable =
            Expressions.parameter(valExpression.getType(),
                list.newName("literal_value"));
        list.add(Expressions.declare(Modifier.FINAL, valVariable, valExpression));

        // Generate one line of code to check whether RexLiteral is null, e.g.,
        // "final boolean literal_isNull = false;"
        final Expression isNullExpression =
            literal.isNull() ? RexImpTable.TRUE_EXPR : RexImpTable.FALSE_EXPR;
        final ParameterExpression isNullVariable = Expressions.parameter(
            Boolean.TYPE, list.newName("literal_isNull"));
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

        // Maintain the map from valueVariable (ParameterExpression) to real Expression
        literalMap.put(valVariable, valExpression);
        final Result result = new Result(isNullVariable, valVariable);
        // Cache RexLiteral's result
        rexResultMap.put(literal, result);
        return result;
    }

    /**
     * Returns an {@code Expression} for null literal without losing its type information.
     */
    private ConstantExpression getTypedNullLiteral(RexLiteral literal) {
        assert literal.isNull();
        Type javaCls = typeFactory.getJavaClass(literal.getType());
        switch (literal.getType().getSqlTypeName()) {
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                javaCls = Integer.class;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
                javaCls = Long.class;
                break;
        }
        return javaCls == null || javaCls == Void.class
            ? RexImpTable.NULL_EXPR
            : Expressions.constant(null, javaCls);
    }

    /**
     * Visit {@code RexCall}. For most {@code SqlOperator}s, we can get the implementor from {@code RexImpTable}.
     * Several operators (e.g., CaseWhen) with special semantics need to be implemented separately.
     */
    @Override public Result visitCall(RexCall call) {
        if (rexResultMap.containsKey(call))
            return rexResultMap.get(call);

        final SqlOperator operator = call.getOperator();
        if (operator == CASE)
            return implementCaseWhen(call);

        if (operator == SEARCH)
            return RexUtil.expandSearch(builder, program, call).accept(this);

        final RexImpTable.RexCallImplementor implementor =
            RexImpTable.INSTANCE.get(operator);
        if (implementor == null)
            throw new RuntimeException("cannot translate call " + call);

        final List<RexNode> operandList = call.getOperands();
        final List<Type> storageTypes = ConverterUtils.internalTypes(operandList);
        final List<Result> operandResults = new ArrayList<>();
        for (int i = 0; i < operandList.size(); i++) {
            final Result operandResult =
                implementCallOperand(operandList.get(i), storageTypes.get(i), this);
            operandResults.add(operandResult);
        }
        callOperandResultMap.put(call, operandResults);
        final Result result = implementor.implement(this, call, operandResults);
        rexResultMap.put(call, result);
        return result;
    }

    /** */
    private static Result implementCallOperand(final RexNode operand,
        final Type storageType, final RexToLixTranslator translator) {
        final Type originalStorageType = translator.currentStorageType;
        translator.currentStorageType = storageType;
        Result operandResult = operand.accept(translator);
        if (storageType != null)
            operandResult = translator.toInnerStorageType(operandResult, storageType);
        translator.currentStorageType = originalStorageType;
        return operandResult;
    }

    /** */
    private static Expression implementCallOperand2(final RexNode operand,
        final Type storageType, final RexToLixTranslator translator) {
        final Type originalStorageType = translator.currentStorageType;
        translator.currentStorageType = storageType;
        final Expression result = translator.translate(operand);
        translator.currentStorageType = originalStorageType;
        return result;
    }

    /**
     * The CASE operator is SQL’s way of handling if/then logic. Different with other {@code RexCall}s, it is not safe
     * to implement its operands first. For example: {@code select case when s=0 then false else 100/s > 0 end from
     * (values (1),(0)) ax(s); }
     */
    private Result implementCaseWhen(RexCall call) {
        final Type returnType = typeFactory.getJavaClass(call.getType());
        final ParameterExpression valVariable =
            Expressions.parameter(returnType,
                list.newName("case_when_value"));
        list.add(Expressions.declare(0, valVariable, null));
        final List<RexNode> operandList = call.getOperands();
        implementRecursively(this, operandList, valVariable, call.getType(), 0);
        final Expression isNullExpression = checkNull(valVariable);
        final ParameterExpression isNullVariable =
            Expressions.parameter(
                Boolean.TYPE, list.newName("case_when_isNull"));
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
        final Result result = new Result(isNullVariable, valVariable);
        rexResultMap.put(call, result);
        return result;
    }

    /**
     * Case statements of the form: {@code CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END}. When {@code a = true},
     * returns {@code b}; when {@code c = true}, returns {@code d}; else returns {@code e}.
     *
     * <p>We generate code that looks like:
     *
     * <blockquote><pre>
     *      int case_when_value;
     *      ......code for a......
     *      if (!a_isNull && a_value) {
     *          ......code for b......
     *          case_when_value = res(b_isNull, b_value);
     *      } else {
     *          ......code for c......
     *          if (!c_isNull && c_value) {
     *              ......code for d......
     *              case_when_value = res(d_isNull, d_value);
     *          } else {
     *              ......code for e......
     *              case_when_value = res(e_isNull, e_value);
     *          }
     *      }
     * </pre></blockquote>
     */
    private void implementRecursively(
        final RexToLixTranslator currentTranslator,
        final List<RexNode> operandList,
        final ParameterExpression valueVariable,
        final RelDataType valueType,
        int pos
    ) {
        final BlockBuilder curBlockBuilder = currentTranslator.getBlockBuilder();
        final List<Type> storageTypes = ConverterUtils.internalTypes(operandList);
        // [ELSE] clause
        if (pos == operandList.size() - 1) {
            Expression res = implementCallOperand2(operandList.get(pos),
                storageTypes.get(pos), currentTranslator);
            curBlockBuilder.add(
                Expressions.statement(
                    Expressions.assign(valueVariable,
                        ConverterUtils.convert(res, valueType))));
            return;
        }
        // Condition code: !a_isNull && a_value
        final RexNode testerNode = operandList.get(pos);
        final Result testerResult = implementCallOperand(testerNode,
            storageTypes.get(pos), currentTranslator);
        final Expression tester = Expressions.andAlso(
            Expressions.not(testerResult.isNullVariable),
            testerResult.valueVariable);
        // Code for {if} branch
        final RexNode ifTrueNode = operandList.get(pos + 1);
        final BlockBuilder ifTrueBlockBuilder =
            new BlockBuilder(true, curBlockBuilder);
        final RexToLixTranslator ifTrueTranslator =
            currentTranslator.setBlock(ifTrueBlockBuilder);
        final Expression ifTrueRes = implementCallOperand2(ifTrueNode,
            storageTypes.get(pos + 1), ifTrueTranslator);
        // Assign the value: case_when_value = ifTrueRes
        ifTrueBlockBuilder.add(
            Expressions.statement(
                Expressions.assign(valueVariable,
                    ConverterUtils.convert(ifTrueRes, valueType))));
        final BlockStatement ifTrue = ifTrueBlockBuilder.toBlock();
        // There is no [ELSE] clause
        if (pos + 1 == operandList.size() - 1) {
            curBlockBuilder.add(
                Expressions.ifThen(tester, ifTrue));
            return;
        }
        // Generate code for {else} branch recursively
        final BlockBuilder ifFalseBlockBuilder =
            new BlockBuilder(true, curBlockBuilder);
        final RexToLixTranslator ifFalseTranslator =
            currentTranslator.setBlock(ifFalseBlockBuilder);
        implementRecursively(ifFalseTranslator, operandList, valueVariable, valueType, pos + 2);
        final BlockStatement ifFalse = ifFalseBlockBuilder.toBlock();
        curBlockBuilder.add(
            Expressions.ifThenElse(tester, ifTrue, ifFalse));
    }

    /** */
    private Result toInnerStorageType(final Result result, final Type storageType) {
        final Expression valExpression =
            ConverterUtils.toInternal(result.valueVariable, storageType);
        if (valExpression.equals(result.valueVariable))
            return result;

        final ParameterExpression valVariable =
            Expressions.parameter(
                valExpression.getType(),
                list.newName(result.valueVariable.name + "_inner_type"));
        list.add(Expressions.declare(Modifier.FINAL, valVariable, valExpression));
        final ParameterExpression isNullVariable = result.isNullVariable;
        return new Result(isNullVariable, valVariable);
    }

    /** {@inheritDoc} */
    @Override public Result visitDynamicParam(RexDynamicParam dynamicParam) {
        final Pair<RexNode, Type> key = Pair.of(dynamicParam, currentStorageType);
        if (rexWithStorageTypeResultMap.containsKey(key))
            return rexWithStorageTypeResultMap.get(key);

        final Type storageType = currentStorageType != null
            ? currentStorageType : typeFactory.getJavaClass(dynamicParam.getType());

        final Type paramType = ((IgniteTypeFactory)typeFactory).getResultClass(dynamicParam.getType());

        final Expression ctxGet = Expressions.call(root, IgniteMethod.CONTEXT_GET_PARAMETER_VALUE.method(),
            Expressions.constant("?" + dynamicParam.getIndex()), Expressions.constant(paramType));

        final Expression valExpression = SqlTypeUtil.isDecimal(dynamicParam.getType()) ?
            ConverterUtils.convertToDecimal(ctxGet, dynamicParam.getType()) : ConverterUtils.convert(ctxGet, storageType);

        final ParameterExpression valVariable =
            Expressions.parameter(valExpression.getType(), list.newName("value_dynamic_param"));
        list.add(Expressions.declare(Modifier.FINAL, valVariable, valExpression));
        final ParameterExpression isNullVariable =
            Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, checkNull(valVariable)));
        final Result result = new Result(isNullVariable, valVariable);
        rexWithStorageTypeResultMap.put(key, result);
        return result;
    }

    /** {@inheritDoc} */
    @Override public Result visitFieldAccess(RexFieldAccess fieldAccess) {
        final Pair<RexNode, Type> key = Pair.of(fieldAccess, currentStorageType);
        if (rexWithStorageTypeResultMap.containsKey(key))
            return rexWithStorageTypeResultMap.get(key);

        final RexNode target = deref(fieldAccess.getReferenceExpr());
        int fieldIdx = fieldAccess.getField().getIndex();
        String fieldName = fieldAccess.getField().getName();
        switch (target.getKind()) {
            case CORREL_VARIABLE:
                if (correlates == null) {
                    throw new RuntimeException("Cannot translate " + fieldAccess
                        + " since correlate variables resolver is not defined");
                }
                final RexToLixTranslator.InputGetter getter =
                    correlates.apply(((RexCorrelVariable)target).getName());
                final Expression input = getter.field(
                    list, fieldIdx, currentStorageType);
                final Expression condition = checkNull(input);
                final ParameterExpression valVariable =
                    Expressions.parameter(input.getType(), list.newName("corInp_value"));
                list.add(Expressions.declare(Modifier.FINAL, valVariable, input));
                final ParameterExpression isNullVariable =
                    Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"));
                final Expression isNullExpression = Expressions.condition(
                    condition,
                    RexImpTable.TRUE_EXPR,
                    checkNull(valVariable));
                list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
                final Result result1 = new Result(isNullVariable, valVariable);
                rexWithStorageTypeResultMap.put(key, result1);
                return result1;
            default:
                RexNode rxIdx =
                    builder.makeLiteral(fieldIdx, typeFactory.createType(int.class), true);
                RexNode rxName =
                    builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
                RexCall accessCall = (RexCall)builder.makeCall(
                    fieldAccess.getType(), SqlStdOperatorTable.STRUCT_ACCESS,
                    ImmutableList.of(target, rxIdx, rxName));
                final Result result2 = accessCall.accept(this);
                rexWithStorageTypeResultMap.put(key, result2);
                return result2;
        }
    }

    /** {@inheritDoc} */
    @Override public Result visitOver(RexOver over) {
        throw new RuntimeException("cannot translate expression " + over);
    }

    /** {@inheritDoc} */
    @Override public Result visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new RuntimeException("Cannot translate " + correlVariable
            + ". Correlated variables should always be referenced by field access");
    }

    /** {@inheritDoc} */
    @Override public Result visitRangeRef(RexRangeRef rangeRef) {
        throw new RuntimeException("cannot translate expression " + rangeRef);
    }

    /** {@inheritDoc} */
    @Override public Result visitSubQuery(RexSubQuery subQuery) {
        throw new RuntimeException("cannot translate expression " + subQuery);
    }

    /** {@inheritDoc} */
    @Override public Result visitTableInputRef(RexTableInputRef fieldRef) {
        throw new RuntimeException("cannot translate expression " + fieldRef);
    }

    /** {@inheritDoc} */
    @Override public Result visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return visitInputRef(fieldRef);
    }

    /** {@inheritDoc} */
    @Override public Result visitLambda(RexLambda lambda) {
        final RexNode expression = lambda.getExpression();
        final List<RexLambdaRef> rexLambdaRefs = lambda.getParameters();

        // Prepare parameter expressions for lambda expression
        final ParameterExpression[] paramExpressions =
            new ParameterExpression[rexLambdaRefs.size()];
        for (int i = 0; i < rexLambdaRefs.size(); i++) {
            final RexLambdaRef rexLambdaRef = rexLambdaRefs.get(i);
            paramExpressions[i] =
                Expressions.parameter(
                    typeFactory.getJavaClass(rexLambdaRef.getType()), rexLambdaRef.getName());
        }

        // Generate code for lambda expression body
        final RexToLixTranslator exprTranslator = setBlock(new BlockBuilder());
        final Result exprResult = expression.accept(exprTranslator);
        exprTranslator.list.add(
            Expressions.return_(null, exprResult.valueVariable));

        // Generate code for lambda expression
        final Expression functionExpression =
            Expressions.lambda(exprTranslator.list.toBlock(), paramExpressions);
        final ParameterExpression valVariable =
            Expressions.parameter(functionExpression.getType(), list.newName("function_value"));
        list.add(Expressions.declare(Modifier.FINAL, valVariable, functionExpression));

        // Generate code for checking whether lambda expression is null
        final Expression isNullExpression = checkNull(valVariable);
        final ParameterExpression isNullVariable =
            Expressions.parameter(Boolean.TYPE, list.newName("function_isNull"));
        list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

        return new Result(isNullVariable, valVariable);
    }

    /** */
    Expression checkNull(Expression expr) {
        if (Primitive.flavor(expr.getType()) == Primitive.Flavor.PRIMITIVE)
            return RexImpTable.FALSE_EXPR;

        return Expressions.equal(expr, RexImpTable.NULL_EXPR);
    }

    /** */
    Expression checkNotNull(Expression expr) {
        if (Primitive.flavor(expr.getType()) == Primitive.Flavor.PRIMITIVE)
            return RexImpTable.TRUE_EXPR;

        return Expressions.notEqual(expr, RexImpTable.NULL_EXPR);
    }

    /** */
    BlockBuilder getBlockBuilder() {
        return list;
    }

    /** */
    Expression getLiteral(Expression literalVariable) {
        return literalMap.get(literalVariable);
    }

    /** Returns the value of a literal. */
    Object getLiteralValue(Expression expr) {
        if (expr instanceof ParameterExpression) {
            final Expression constantExpr = literalMap.get(expr);
            return getLiteralValue(constantExpr);
        }
        if (expr instanceof ConstantExpression)
            return ((ConstantExpression)expr).value;

        return null;
    }

    /** */
    List<Result> getCallOperandResult(RexCall call) {
        return callOperandResultMap.get(call);
    }

    /** Translates a field of an input to an expression. */
    public interface InputGetter {
        /** */
        Expression field(BlockBuilder list, int index, Type storageType);
    }

    /** Result of translating a {@code RexNode}. */
    public static class Result {
        /** */
        final ParameterExpression isNullVariable;

        /** */
        final ParameterExpression valueVariable;

        /** */
        public Result(ParameterExpression isNullVariable,
            ParameterExpression valueVariable) {
            this.isNullVariable = isNullVariable;
            this.valueVariable = valueVariable;
        }
    }

    /**
     * Handle checked Exceptions declared in Method. In such case,
     * method call should be wrapped in a try...catch block.
     * "
     *      final Type method_call;
     *      try {
     *        method_call = callExpr
     *      } catch (Exception e) {
     *        throw new RuntimeException(e);
     *      }
     * "
     */
    Expression handleMethodCheckedExceptions(Expression callExpr) {
        // Try statement
        ParameterExpression methodCall = Expressions.parameter(
            callExpr.getType(), list.newName("method_call"));
        list.add(Expressions.declare(Modifier.FINAL, methodCall, null));
        Statement st = Expressions.statement(Expressions.assign(methodCall, callExpr));
        // Catch Block, wrap checked exception in unchecked exception
        ParameterExpression e = Expressions.parameter(0, Exception.class, "e");
        Expression uncheckedEx = Expressions.new_(RuntimeException.class, e);
        CatchBlock cb = Expressions.catch_(e, Expressions.throw_(uncheckedEx));
        list.add(Expressions.tryCatch(st, cb));
        return methodCall;
    }
}
