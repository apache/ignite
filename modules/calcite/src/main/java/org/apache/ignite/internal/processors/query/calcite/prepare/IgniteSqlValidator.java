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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.OrderByScope;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Static;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteScalarFunction;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlDecimalLiteral;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.OtherType;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMath;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/** Validator. */
@Value.Enclosing
public class IgniteSqlValidator extends SqlValidatorImpl {
    /** **/
    private static final int MAX_LENGTH_OF_ALIASES = 256;

    /** **/
    private static final Set<SqlKind> HUMAN_READABLE_ALIASES_FOR;

    static {
        EnumSet<SqlKind> kinds = EnumSet.noneOf(SqlKind.class);

        kinds.addAll(SqlKind.AGGREGATE);
        kinds.addAll(SqlKind.BINARY_ARITHMETIC);
        kinds.addAll(SqlKind.FUNCTION);

        kinds.add(SqlKind.CEIL);
        kinds.add(SqlKind.FLOOR);
        kinds.add(SqlKind.LITERAL);

        HUMAN_READABLE_ALIASES_FOR = Collections.unmodifiableSet(kinds);
    }

    /** Dynamic parameters. */
    @Nullable private final Object[] parameters;

    /** */
    private final RelDataType nullType;

    /** */
    private final @Nullable IgniteSqlSemantics sqlSem;

    /** */
    private final boolean allowUdfCallsWithoutParentheses;

    /**
     * Creates a validator.
     *
     * @param opTab Operator table.
     * @param catalogReader Catalog reader.
     * @param typeFactory Type factory.
     * @param cfg Config.
     * @param parameters Dynamic parameters.
     * @param sqlSem SQL semantics.
     * @param allowUdfCallsWithoutParentheses Whether parameterless UDFs can be called without parentheses.
     */
    public IgniteSqlValidator(
        SqlOperatorTable opTab,
        CalciteCatalogReader catalogReader,
        IgniteTypeFactory typeFactory,
        SqlValidator.Config cfg,
        @Nullable Object[] parameters,
        @Nullable IgniteSqlSemantics sqlSem,
        boolean allowUdfCallsWithoutParentheses
    ) {
        super(opTab, catalogReader, typeFactory, cfg);

        this.parameters = parameters;
        this.sqlSem = sqlSem;
        this.allowUdfCallsWithoutParentheses = allowUdfCallsWithoutParentheses;

        nullType = typeFactory.createSqlType(SqlTypeName.NULL);
    }

    /** {@inheritDoc} */
    @Override public void validateInsert(SqlInsert insert) {
        validateTableModify(insert.getTargetTable());

        if (insert.getTargetColumnList() == null)
            insert.setOperand(3, inferColumnList(insert));
        else
            validateInsertTargets(insert);

        super.validateInsert(insert);
    }

    /** {@inheritDoc} */
    @Override public void validateWithItem(SqlWithItem withItem) {
        super.validateWithItem(withItem);

        RecursiveCteValidator.validate(this, withItem);
    }

    /**
     * Validates insert target columns to ensure they do not contain the version column.
     *
     * @param insert Insert statement.
     */
    private void validateInsertTargets(SqlInsert insert) {
        for (SqlNode node : insert.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier)node;

            validateVersionColumnDmlTarget(id);
        }
    }

    /** {@inheritDoc} */
    @Override public void validateUpdate(SqlUpdate call) {
        validateTableModify(call.getTargetTable());

        validateUpdateFields(call);

        super.validateUpdate(call);
    }

    /** {@inheritDoc} */
    @Override public void validateDelete(SqlDelete call) {
        validateTableModify(call.getTargetTable());

        super.validateDelete(call);
    }

    /** {@inheritDoc} */
    @Override public void validateMerge(SqlMerge call) {
        validateTableModify(call.getTargetTable());

        super.validateMerge(call);
    }

    /** Validates table modify operation. */
    private void validateTableModify(SqlNode table) {
        final SqlValidatorTable targetTable = getCatalogReader().getTable(((SqlIdentifier)table).names);

        if (targetTable == null)
            throw newValidationError(table, Static.RESOURCE.objectNotFound(table.toString()));

        if (!targetTable.unwrap(IgniteTable.class).isModifiable())
            throw newValidationError(table, IgniteResource.INSTANCE.modifyTableNotSupported(table.toString()));
    }

    /** {@inheritDoc} */
    @Override public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() != SqlTypeName.DECIMAL)
            super.validateLiteral(literal);
    }

    /** {@inheritDoc} */
    @Override protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlIdentifier targetTable = (SqlIdentifier)call.getTargetTable();
        final SqlValidatorTable table = getCatalogReader().getTable(targetTable.names);

        if (table == null)
            throw newValidationError(call, Static.RESOURCE.objectNotFound(targetTable.toString()));

        SqlIdentifier alias = call.getAlias() != null ? call.getAlias() :
            new SqlIdentifier(deriveAlias(targetTable, 0), SqlParserPos.ZERO);

        RelDataType updateRowType = table.unwrap(IgniteTable.class).descriptor()
            .selectForUpdateRowType(typeFactory());

        for (RelDataTypeField field : updateRowType.getFieldList()) {
            if (QueryUtils.VER_FIELD_NAME.equals(field.getName())) {
                selectList.add(SqlValidatorUtil.addAlias(
                    SqlLiteral.createNull(SqlParserPos.ZERO),
                    "__IGNITE_DML" + field.getName()
                ));
            }
            else
                selectList.add(alias.plus(field.getName(), SqlParserPos.ZERO));
        }

        int ordinal = 0;
        // Force unique aliases to avoid a duplicate for Y with SET X=Y
        for (SqlNode exp : call.getSourceExpressionList())
            selectList.add(SqlValidatorUtil.addAlias(exp, SqlUtil.deriveAliasFromOrdinal(ordinal++)));

        SqlNode srcTable = call.getTargetTable();

        if (call.getAlias() != null) {
            srcTable =
                SqlValidatorUtil.addAlias(
                    srcTable,
                    call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, srcTable,
            call.getCondition(), null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
        final SqlNodeList selectList = SqlNodeList.of(
            new SqlIdentifier(QueryUtils.KEY_FIELD_NAME, SqlParserPos.ZERO));

        SqlNode srcTable = call.getTargetTable();

        if (call.getAlias() != null) {
            srcTable =
                SqlValidatorUtil.addAlias(
                    srcTable,
                    call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, srcTable,
            call.getCondition(), null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void validateSelect(SqlSelect select, RelDataType targetRowType) {
        if (allowUdfCallsWithoutParentheses && select.getFetch() != null)
            select.setFetch(rewriteScalarUdfCalls(select.getFetch(), getEmptyScope(), null));

        super.validateSelect(select, targetRowType);

        validateFetch(select, "fetch / limit");
        validateFetchOffset(select.getOffset(), "offset");
    }

    /** {@inheritDoc} */
    @Override public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
        return super.expand(rewriteScalarUdfCalls(expr, scope, null), scope);
    }

    /** {@inheritDoc} */
    @Override protected void validateGroupClause(SqlSelect select) {
        if (allowUdfCallsWithoutParentheses) {
            // FROM has been validated. Rewrite projections before Calcite expands GROUP BY aliases and ordinals.
            SqlValidatorScope scope = getWhereScope(select);
            SqlNodeList projections = select.getSelectList();

            for (int i = 0; i < projections.size(); i++) {
                SqlNode original = projections.get(i);
                SqlNode rewritten = rewriteScalarUdfCalls(original, scope, null);

                if (rewritten != original && original instanceof SqlIdentifier) {
                    SqlIdentifier id = (SqlIdentifier)original;

                    rewritten = SqlStdOperatorTable.AS.createCall(id.getParserPosition(), rewritten,
                        id.getComponent(id.names.size() - 1));
                }

                if (rewritten != original)
                    projections.set(i, rewritten);
            }

            if (select.getGroup() != null) {
                select.setGroupBy((SqlNodeList)rewriteScalarUdfCalls(select.getGroup(), getGroupScope(select),
                    config().conformance().isGroupByAlias() ? select : null));
            }
        }

        super.validateGroupClause(select);
    }

    /** {@inheritDoc} */
    @Override protected void validateHavingClause(SqlSelect select) {
        if (select.getHaving() != null) {
            select.setHaving(rewriteScalarUdfCalls(select.getHaving(), getHavingScope(select),
                config().conformance().isHavingAlias() ? select : null));
        }

        super.validateHavingClause(select);
    }

    /** {@inheritDoc} */
    @Override protected void validateQualifyClause(SqlSelect select) {
        if (select.getQualify() != null)
            select.setQualify(rewriteScalarUdfCalls(select.getQualify(), getSelectScope(select), select));

        super.validateQualifyClause(select);
    }

    /** {@inheritDoc} */
    @Override protected void validateValues(SqlCall values, RelDataType targetRowType, SqlValidatorScope scope) {
        if (allowUdfCallsWithoutParentheses) {
            SqlValidatorScope valuesScope = scopes.getOrDefault(values, scope);

            for (int i = 0; i < values.operandCount(); i++)
                values.setOperand(i, rewriteScalarUdfCalls(values.operand(i), valuesScope, null));
        }

        super.validateValues(values, targetRowType, scope);
    }

    /** Replaces scalar UDF names in expressions after checking visible columns and aliases. */
    private SqlNode rewriteScalarUdfCalls(SqlNode expr, SqlValidatorScope scope, @Nullable SqlSelect aliases) {
        if (!allowUdfCallsWithoutParentheses)
            return expr;

        return expr.accept(new SqlShuttle() {
            @Override public SqlNode visit(SqlIdentifier id) {
                if (id.isStar())
                    return id;

                SqlOperator op = parameterlessUdf(id);

                if (op == null || hasColumnReference(id, scope, aliases))
                    return id;

                SqlCall call = op.createCall(id.getParserPosition());

                setOriginal(call, id);

                return call;
            }

            @Override public SqlNode visit(SqlCall call) {
                // Subqueries and lambda bodies have their own scopes.
                if (call.isA(SqlKind.QUERY) || call.getKind() == SqlKind.LAMBDA || call.getKind() == SqlKind.SCALAR_QUERY)
                    return call;

                CallCopyingArgHandler hnd = new CallCopyingArgHandler(call, false);

                call.getOperator().acceptCall(this, call, true, hnd);

                return hnd.result();
            }
        });
    }

    /** Checks for a column match without suppressing ambiguity errors in Calcite's normal resolution. */
    private boolean hasColumnReference(SqlIdentifier id, SqlValidatorScope scope, @Nullable SqlSelect aliases) {
        SqlNameMatcher matcher = getCatalogReader().nameMatcher();

        if (id.isSimple()) {
            String name = id.getSimple();

            if (scope instanceof OrderByScope && scope.resolveColumn(name, id) != null)
                return true;

            if (!scope.findQualifyingTableNames(name, id, matcher).isEmpty())
                return true;

            return aliases != null && aliases.getSelectList().stream().anyMatch(item -> {
                String alias = SqlValidatorUtil.alias(item);

                return alias != null && matcher.matches(alias, name);
            });
        }

        SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();

        scope.resolve(id.names, matcher, true, resolved);

        return resolved.count() != 0;
    }

    /** Finds a zero-argument Ignite scalar UDF. */
    private @Nullable SqlOperator parameterlessUdf(SqlIdentifier id) {
        List<SqlOperator> ops = new ArrayList<>();

        CalciteCatalogReader catalogReader = (CalciteCatalogReader)getCatalogReader();

        catalogReader.lookupOperatorOverloads(id, SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, ops,
            catalogReader.nameMatcher());

        for (SqlOperator op : ops) {
            if (op instanceof SqlUserDefinedFunction) {
                Function func = ((SqlUserDefinedFunction)op).getFunction();

                if (func instanceof IgniteScalarFunction && func.getParameters().isEmpty())
                    return op;
            }
        }

        return null;
    }

    /** Validate fetch expression. */
    // TODO: https://issues.apache.org/jira/browse/CALCITE-7592
    //  Remove this method after upgrading to Calcite 1.43.
    private void validateFetch(SqlSelect select, String clauseName) {
        SqlNode fetch = select.getFetch();

        if (fetch == null)
            return;

        if (SqlUtil.isNullLiteral(fetch, true))
            throw newValidationError(fetch, IgniteResource.INSTANCE.illegalFetchLimit(clauseName));

        validateFetchExpression(fetch, clauseName);

        SqlValidatorScope scope = getEmptyScope();

        inferUnknownTypes(typeFactory().createSqlType(SqlTypeName.DECIMAL), scope, fetch);
        RelDataType type = deriveType(scope, fetch);

        if (type.getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC && !(fetch instanceof SqlDynamicParam))
            throw newValidationError(fetch, IgniteResource.INSTANCE.illegalFetchLimit(clauseName));

        validateFetchOffset(fetch, clauseName);
    }

    /** Reject column references in a fetch expression. */
    // TODO: https://issues.apache.org/jira/browse/CALCITE-7592
    //  Remove this method after upgrading to Calcite 1.43.
    private void validateFetchExpression(SqlNode node, String clauseName) {
        if (node instanceof SqlIdentifier) {
            if (makeNullaryCall((SqlIdentifier)node) == null)
                throw newValidationError(node, IgniteResource.INSTANCE.illegalFetchLimit(clauseName));

            return;
        }

        if (node instanceof SqlNodeList) {
            for (SqlNode child : (SqlNodeList)node)
                validateFetchExpression(child, clauseName);
        }
        else if (node instanceof SqlCall call) {
            for (SqlNode child : call.getOperandList()) {
                if (child != null)
                    validateFetchExpression(child, clauseName);
            }
        }
    }

    /**
     * Validate fetch/offset params restrictions.
     *
     * @param n          Node to check.
     * @param clauseName Clause name.
     */
    private void validateFetchOffset(@Nullable SqlNode n, String clauseName) {
        if (n == null)
            return;

        if (n instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral)n;

            if (literal.getTypeName().getFamily() != SqlTypeFamily.NUMERIC)
                throw newValidationError(n, IgniteResource.INSTANCE.illegalFetchLimit(clauseName));

            BigDecimal offsetFetchLimit = literal.bigDecimalValue();

            checkLimitOffset(offsetFetchLimit, n, clauseName);
        }
        else if (n instanceof SqlDynamicParam dynamicParam) {
            // Dynamic parameters are nullable.
            RelDataType expectType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DECIMAL), true);

            if (definedDynParam(dynamicParam)) {
                Object param = parameters[dynamicParam.getIndex()];

                if (!(param instanceof Number)) {
                    Resources.ExInst<SqlValidatorException> err;

                    if (param == null)
                        err = IgniteResource.INSTANCE.incorrectDynamicParameterType(SqlTypeName.BIGINT.toString(), "null");
                    else {
                        SqlTypeName paramType = typeFactory().createType(param.getClass()).getSqlTypeName();
                        err = IgniteResource.INSTANCE.incorrectDynamicParameterType(SqlTypeName.BIGINT.toString(), paramType.getName());
                    }

                    throw newValidationError(n, err);
                }
                else
                    checkLimitOffset((Number)param, n, clauseName);

                setValidatedNodeType(dynamicParam, expectType);
            }
            else {
                expectType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
                setValidatedNodeType(dynamicParam, expectType);
            }
        }
    }

    /** Returns {@code true} if the given dynamic parameter has value set. */
    private boolean definedDynParam(SqlDynamicParam param) {
        return param.getIndex() < parameters.length;
    }

    /** */
    private void checkLimitOffset(Number offsetFetchLimit, SqlNode n, String nodeName) {
        try {
            BigDecimal val = IgniteMath.convertToBigDecimal(offsetFetchLimit);

            if (val.signum() < 0)
                throw new IllegalArgumentException("Negative value for " + nodeName);

            IgniteSqlSemantics.convertPaginationValueToLong(val, sqlSem);
        }
        catch (RuntimeException e) {
            throw newValidationError(n, IgniteResource.INSTANCE.illegalFetchLimit(nodeName));
        }
    }

    /** {@inheritDoc} */
    @Override protected void validateNamespace(SqlValidatorNamespace namespace, RelDataType targetRowType) {
        SqlValidatorTable table = namespace.getTable();

        if (table != null) {
            IgniteCacheTable igniteTable = table.unwrap(IgniteCacheTable.class);

            if (igniteTable != null)
                igniteTable.ensureCacheStarted();
        }

        super.validateNamespace(namespace, targetRowType);
    }

    /** {@inheritDoc} */
    @Override public void validateCall(SqlCall call, SqlValidatorScope scope) {
        if (call.getKind() == SqlKind.AS) {
            final String alias = deriveAlias(call, 0);

            if (QueryUtils.isSystemFieldNameIgnoreCase(alias))
                throw newValidationError(call, IgniteResource.INSTANCE.illegalAlias(alias));
        }
        else if (call.getKind() == SqlKind.CAST) {
            if (call.getOperandList().size() > 2)
                throw newValidationError(call, IgniteResource.INSTANCE.invalidCastParameters());
        }
        else if (call.getKind() == SqlKind.OVER && call.operand(1) instanceof SqlWindow) {
            SqlLiteral exclude = call.<SqlWindow>operand(1).getExclude();
            if (exclude != null && !SqlWindow.isExcludeNoOthers(exclude)) {
                String clause = exclude.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
                throw newValidationError(exclude, IgniteResource.INSTANCE.unsupportedClause(clause));
            }
        }

        super.validateCall(call, scope);
    }

    /** {@inheritDoc} */
    @Override public String deriveAlias(SqlNode node, int ordinal) {
        if (node.isA(HUMAN_READABLE_ALIASES_FOR)) {
            String alias = node.toSqlString(c -> c.withDialect(CalciteSqlDialect.DEFAULT)
                .withQuoteAllIdentifiers(false)
                .withAlwaysUseParentheses(false)
                .withClauseStartsLine(false)
            ).getSql();

            return alias.substring(0, Math.min(alias.length(), MAX_LENGTH_OF_ALIASES));
        }

        return super.deriveAlias(node, ordinal);
    }

    /** {@inheritDoc} */
    @Override public void validateAggregateParams(SqlCall aggCall,
        @Nullable SqlNode filter, @Nullable SqlNodeList distinctList,
        @Nullable SqlNodeList orderList, SqlValidatorScope scope) {
        validateAggregateFunction(aggCall, (SqlAggFunction)aggCall.getOperator());

        super.validateAggregateParams(aggCall, filter, null, orderList, scope);
    }

    /** {@inheritDoc} */
    @Override protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        if (join.getJoinType() == JoinType.ASOF || join.getJoinType() == JoinType.LEFT_ASOF)
            throw newValidationError(join, IgniteResource.INSTANCE.unsupportedJoinType(join.getJoinType().name().replace('_', ' ')));

        super.validateJoin(join, scope);
    }

    /** {@inheritDoc} */
    @Override protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
        if (node instanceof SqlWithItem)
            RecursiveCteRewriter.rewriteOrderBy((SqlWithItem)node);

        if (node instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy)node;

            if (orderBy.fetch != null) {
                // SqlOrderBy does not implement setOperand(). Rewrite FETCH before Calcite visits its operands,
                // otherwise rewrites such as COALESCE to CASE fail when Calcite tries to replace the FETCH operand.
                SqlNode fetch = performUnconditionalRewrites(orderBy.fetch, false);

                if (fetch != orderBy.fetch) {
                    node = new SqlOrderBy(orderBy.getParserPosition(), orderBy.query, orderBy.orderList,
                        orderBy.offset, fetch);
                }
            }
        }

        // Workaround for https://issues.apache.org/jira/browse/CALCITE-4923
        if (node instanceof SqlSelect) {
            SqlSelect select = (SqlSelect)node;

            if (select.getFrom() instanceof SqlJoin) {
                boolean hasStar = false;

                for (SqlNode expr : select.getSelectList()) {
                    if (expr instanceof SqlIdentifier && ((SqlIdentifier)expr).isStar()
                        && ((SqlIdentifier)expr).names.size() == 1)
                        hasStar = true;
                }

                performJoinRewrites((SqlJoin)select.getFrom(), hasStar);
            }
        }

        node = super.performUnconditionalRewrites(node, underFrom);

        if (node instanceof SqlWithItem)
            RecursiveCteRewriter.inferRecursion((SqlWithItem)node);

        if (config() instanceof Config && ((Config)config()).sqlNodeRewriter() != null)
            node = ((Config)config()).sqlNodeRewriter().rewrite(this, node);

        return node;
    }

    /** Rewrites JOIN clause if required */
    private void performJoinRewrites(SqlJoin join, boolean hasStar) {
        if (join.getLeft() instanceof SqlJoin)
            performJoinRewrites((SqlJoin)join.getLeft(), hasStar || join.isNatural());

        if (join.getRight() instanceof SqlJoin)
            performJoinRewrites((SqlJoin)join.getRight(), hasStar || join.isNatural());

        // Join with USING should be rewriten if SELECT conatins "star" in projects, NATURAL JOIN also has other issues
        // and should be rewritten in any case.
        if (join.isNatural() || (join.getConditionType() == JoinConditionType.USING && hasStar)) {
            // Default Calcite validator can't expand "star" for NATURAL joins and joins with USING if some columns
            // of join sources are filtered out by the addToSelectList method, and the count of columns in the
            // selectList not equals to the count of fields in the corresponding rowType. Since we do filtering in the
            // addToSelectList method (exclude _KEY and _VAL columns), to workaround the expandStar limitation we can
            // wrap each table to a subquery. In this case columns will be filtered out on the subquery level and
            // rowType of the subquery will have the same cardinality as selectList.
            join.setLeft(rewriteTableToQuery(join.getLeft()));
            join.setRight(rewriteTableToQuery(join.getRight()));
        }
    }

    /** Wrap table to subquery "SELECT * FROM table". */
    private SqlNode rewriteTableToQuery(SqlNode from) {
        SqlNode src = from.getKind() == SqlKind.AS ? ((SqlCall)from).getOperandList().get(0) : from;

        if (src.getKind() == SqlKind.IDENTIFIER || src.getKind() == SqlKind.TABLE_REF) {
            String alias = deriveAlias(from, 0);

            SqlSelect expandedQry = new SqlSelect(SqlParserPos.ZERO, null,
                SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)), src, null, null, null,
                null, null, null, null, null);

            return SqlValidatorUtil.addAlias(expandedQry, alias);
        }
        else
            return from;
    }

    /** {@inheritDoc} */
    @Override protected void addToSelectList(
        List<SqlNode> list,
        Set<String> aliases,
        List<Map.Entry<String, RelDataType>> fieldList,
        SqlNode exp,
        SelectScope scope,
        boolean includeSysVars
    ) {
        if (!includeSysVars && exp.getKind() == SqlKind.IDENTIFIER) {
            String alias = deriveAlias(exp, 0);

            // The version column is never exposed via SELECT *.
            if (QueryUtils.VER_FIELD_NAME.equalsIgnoreCase(alias))
                return;

            if (QueryUtils.isSystemFieldNameIgnoreCase(alias)) {
                SqlQualified qualified = scope.fullyQualify((SqlIdentifier)exp);

                if (qualified.namespace == null)
                    return;

                if (qualified.namespace.getTable() != null) {
                    // If child is table and has only system fields, expand star to these fields.
                    // Otherwise, expand star to non-system fields only.
                    for (RelDataTypeField fld : qualified.namespace.getRowType().getFieldList()) {
                        if (!isSystemField(fld))
                            return;
                    }
                }
            }
        }

        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSysVars);
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemField(RelDataTypeField field) {
        return QueryUtils.isSystemFieldNameIgnoreCase(field.getName());
    }

    /** */
    private void validateAggregateFunction(SqlCall call, SqlAggFunction aggFunction) {
        switch (aggFunction.kind) {
            case COUNT:
                if (call.operandCount() > 1)
                    throw newValidationError(call, RESOURCE.invalidArgCount(aggFunction.getName(), 1));

                return;
            case SUM:
            case AVG:
            case MIN:
            case MAX:
            case ANY_VALUE:
            case ARRAY_AGG:
            case ARRAY_CONCAT_AGG:
            case GROUP_CONCAT:
            case LISTAGG:
            case STRING_AGG:
            case BIT_AND:
            case BIT_OR:
            case BIT_XOR:
            case ROW_NUMBER:
            case DENSE_RANK:
            case RANK:
            case PERCENT_RANK:
            case CUME_DIST:
            case LAG:
            case LEAD:
            case FIRST_VALUE:
            case LAST_VALUE:
            case NTILE:
            case NTH_VALUE:
            case OTHER_FUNCTION:
                return;
            default:
                throw newValidationError(call,
                    IgniteResource.INSTANCE.unsupportedAggregationFunction(aggFunction.getName()));
        }
    }

    /** */
    private SqlNodeList inferColumnList(SqlInsert call) {
        final SqlValidatorTable table = table(validatedNamespace(call, unknownType));

        if (table == null)
            return null;

        final CacheTableDescriptor desc = table.unwrap(CacheTableDescriptor.class);

        if (desc == null)
            return null;

        final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);

        for (RelDataTypeField field : desc.insertRowType(typeFactory()).getFieldList())
            columnList.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));

        return columnList;
    }

    /** */
    private void validateUpdateFields(SqlUpdate call) {
        if (call.getTargetColumnList() == null)
            return;

        final SqlValidatorNamespace ns = validatedNamespace(call, unknownType);

        final SqlValidatorTable table = table(ns);

        if (table == null)
            return;

        final CacheTableDescriptor desc = table.unwrap(CacheTableDescriptor.class);

        if (desc == null)
            return;

        final RelDataType baseType = table.getRowType();
        final RelOptTable relOptTable = relOptTable(ns);

        for (SqlNode node : call.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier)node;
            validateVersionColumnDmlTarget(id);

            RelDataTypeField target = SqlValidatorUtil.getTargetField(
                baseType, typeFactory(), id, getCatalogReader(), relOptTable);

            if (target == null)
                throw newValidationError(id,
                    RESOURCE.unknownTargetColumn(id.toString()));

            if (!desc.isUpdateAllowed(relOptTable, target.getIndex()))
                throw newValidationError(id,
                    IgniteResource.INSTANCE.cannotUpdateField(id.toString()));
        }
    }

    /** */
    private SqlValidatorTable table(SqlValidatorNamespace ns) {
        RelOptTable relOptTable = relOptTable(ns);

        if (relOptTable != null)
            return relOptTable.unwrap(SqlValidatorTable.class);

        return ns.getTable();
    }

    /** */
    private RelOptTable relOptTable(SqlValidatorNamespace ns) {
        return SqlValidatorUtil.getRelOptTable(
            ns, getCatalogReader().unwrap(Prepare.CatalogReader.class), null, null);
    }

    /** */
    private SqlValidatorNamespace validatedNamespace(SqlNode node, RelDataType targetType) {
        SqlValidatorNamespace ns = getNamespace(node);
        validateNamespace(ns, targetType);
        return ns;
    }

    /** */
    private IgniteTypeFactory typeFactory() {
        return (IgniteTypeFactory)typeFactory;
    }

    /** {@inheritDoc} */
    @Override public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        if (expr instanceof SqlDynamicParam) {
            RelDataType type = deriveDynamicParameterType((SqlDynamicParam)expr, nullType);

            if (type != null)
                return type;
        }

        return super.deriveType(scope, expr);
    }

    /** */
    private void validateVersionColumnDmlTarget(SqlIdentifier id) {
        String fieldName = id.names.get(id.names.size() - 1);

        if (QueryUtils.VER_FIELD_NAME.equalsIgnoreCase(fieldName))
            throw newValidationError(id, IgniteResource.INSTANCE.cannotModifySystemColumn(id.toString()));
    }

    /** @return A derived type or {@code null} if unable to determine. */
    @Nullable private RelDataType deriveDynamicParameterType(SqlDynamicParam node, RelDataType nullValType) {
        RelDataType type = getValidatedNodeTypeIfKnown(node);

        // Do not clarify the widest type for any value.
        if (type instanceof OtherType)
            return type;

        if (parameters == null || node.getIndex() >= parameters.length)
            return null;

        Object val = parameters[node.getIndex()];

        if (val == null && type != null)
            return type;

        type = val == null
            ? typeFactory().createTypeWithNullability(nullValType, true)
            : typeFactory().createTypeWithNullability(typeFactory().toSql(typeFactory().createType(val.getClass())), true);

        setValidatedNodeType(node, type);

        return type;
    }

    /** {@inheritDoc} */
    @Override protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlDynamicParam && !(inferredType instanceof OtherType)
            && deriveDynamicParameterType((SqlDynamicParam)node, unknownType.equals(inferredType) ? nullType : inferredType) != null)
            return;

        if (node instanceof SqlCase || node.isA(SqlKind.QUERY)) {
            // Let Calcite handle CASE operands and defer inference in subqueries to their own validation.
            super.inferUnknownTypes(inferredType, scope, node);
        }
        else if (node instanceof SqlCall) {
            final SqlValidatorScope newScope = scopes.get(node);

            if (newScope != null)
                scope = newScope;

            final SqlCall call = (SqlCall)node;

            if (allowUdfCallsWithoutParentheses && call.getOperator() instanceof SqlFunction
                && ((SqlFunction)call.getOperator()).getFunctionType().isTableFunction()) {
                // The scope supplied by Calcite here contains the inputs visible to the table function.
                SqlCall rewritten = (SqlCall)rewriteScalarUdfCalls(call, scope, null);

                if (rewritten != call) {
                    for (int i = 0; i < call.operandCount(); i++)
                        call.setOperand(i, rewritten.operand(i));
                }
            }

            final SqlOperandTypeInference operandTypeInference = call.getOperator().getOperandTypeInference();
            final SqlOperandTypeChecker operandTypeChecker = call.getOperator().getOperandTypeChecker();
            final SqlCallBinding callBinding = new SqlCallBinding(this, scope, call);
            final List<SqlNode> operands = callBinding.operands();
            final RelDataType[] operandTypes = new RelDataType[operands.size()];

            Arrays.fill(operandTypes, unknownType);

            if (operandTypeInference != null)
                operandTypeInference.inferOperandTypes(callBinding, inferredType, operandTypes);
            else if (operandTypeChecker instanceof FamilyOperandTypeChecker) {
                // Infer operand types from checker for dynamic parameters if it's possible.
                FamilyOperandTypeChecker checker = (FamilyOperandTypeChecker)operandTypeChecker;

                for (int i = 0; i < checker.getOperandCountRange().getMax(); i++) {
                    if (i >= operandTypes.length)
                        break;

                    SqlTypeFamily family = checker.getOperandSqlTypeFamily(i);
                    RelDataType type = family.getDefaultConcreteType(typeFactory());

                    if (type != null && operands.get(i) instanceof SqlDynamicParam)
                        operandTypes[i] = type;
                }
            }

            for (int i = 0; i < operands.size(); ++i) {
                final SqlNode operand = operands.get(i);

                if (operand != null)
                    inferUnknownTypes(operandTypes[i], scope, operand);
            }
        }
        else
            super.inferUnknownTypes(inferredType, scope, node);
    }

    /** {@inheritDoc} */
    @Override public SqlLiteral resolveLiteral(SqlLiteral literal) {
        if (literal instanceof SqlNumericLiteral && literal.createSqlType(typeFactory).getSqlTypeName() == SqlTypeName.BIGINT) {
            BigDecimal bd = literal.getValueAs(BigDecimal.class);

            if (bd.scale() == 0) {
                try {
                    bd.longValueExact();
                }
                catch (ArithmeticException e) {
                    return new IgniteSqlDecimalLiteral((SqlNumericLiteral)literal);
                }
            }
        }

        return super.resolveLiteral(literal);
    }

    /** {@inheritDoc} */
    @Override protected void validateUnnest(SqlCall call, SqlValidatorScope scope, RelDataType targetRowType) {
        if (call.operandCount() > 1)
            throw newValidationError(call, RESOURCE.invalidArgCount(call.getOperator().getName(), 1));

        if (allowUdfCallsWithoutParentheses) {
            // The registered scope contains the preceding inputs, but not the UNNEST being validated.
            scope = scopes.getOrDefault(call, scope);
        }

        super.validateUnnest(call, scope, targetRowType);
    }

    /**
     * Interface to define extension to the configuration for a IgniteSqlValidator.
     */
    @Value.Immutable(singleton = false)
    public interface Config extends SqlValidator.Config {
        /** Default configuration. */
        Config DFLT = ImmutableIgniteSqlValidator.Config.builder().from(DEFAULT).build();

        /** Gets custom call rewriter. */
        @Value.Default
        @Nullable default IgniteSqlNodeRewriter sqlNodeRewriter() {
            return null;
        }

        /** Sets custom call rewriter. */
        Config withSqlNodeRewriter(@Nullable IgniteSqlNodeRewriter rewriter);

        /** Ovverride due to lost @Nullable annotation from the super-class by immutables generator. */
        @Override @Nullable SqlTypeCoercionRule typeCoercionRules();

        /** Ovverride due to lost @Nullable annotation from the super-class by immutables generator. */
        @Override Config withTypeCoercionRules(@Nullable SqlTypeCoercionRule rules);
    }
}
