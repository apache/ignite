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
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/** Validator. */
public class IgniteSqlValidator extends SqlValidatorImpl {
    /** Decimal of Integer.MAX_VALUE for fetch/offset bounding. */
    private static final BigDecimal DEC_INT_MAX = BigDecimal.valueOf(Integer.MAX_VALUE);

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
    Object[] parameters;

    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param config        Config
     * @param parameters    Dynamic parameters
     */
    public IgniteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader,
        IgniteTypeFactory typeFactory, SqlValidator.Config config, Object[] parameters) {
        super(opTab, catalogReader, typeFactory, config);

        this.parameters = parameters;
    }

    /** {@inheritDoc} */
    @Override public void validateInsert(SqlInsert insert) {
        validateTableModify(insert.getTargetTable());

        if (insert.getTargetColumnList() == null)
            insert.setOperand(3, inferColumnList(insert));

        super.validateInsert(insert);
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

        SqlIdentifier alias = call.getAlias() != null ? call.getAlias() :
            new SqlIdentifier(deriveAlias(targetTable, 0), SqlParserPos.ZERO);

        table.unwrap(IgniteTable.class).descriptor().selectForUpdateRowType((IgniteTypeFactory)typeFactory)
            .getFieldNames().stream()
            .map(name -> alias.plus(name, SqlParserPos.ZERO))
            .forEach(selectList::add);

        int ordinal = 0;
        // Force unique aliases to avoid a duplicate for Y with SET X=Y
        for (SqlNode exp : call.getSourceExpressionList())
            selectList.add(SqlValidatorUtil.addAlias(exp, SqlUtil.deriveAliasFromOrdinal(ordinal++)));

        SqlNode sourceTable = call.getTargetTable();

        if (call.getAlias() != null) {
            sourceTable =
                SqlValidatorUtil.addAlias(
                    sourceTable,
                    call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
            call.getCondition(), null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
        final SqlNodeList selectList = SqlNodeList.of(
            new SqlIdentifier(QueryUtils.KEY_FIELD_NAME, SqlParserPos.ZERO));

        SqlNode sourceTable = call.getTargetTable();

        if (call.getAlias() != null) {
            sourceTable =
                SqlValidatorUtil.addAlias(
                    sourceTable,
                    call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
            call.getCondition(), null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void validateSelect(SqlSelect select, RelDataType targetRowType) {
        checkIntegerLimit(select.getFetch(), "fetch / limit");
        checkIntegerLimit(select.getOffset(), "offset");

        super.validateSelect(select, targetRowType);
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

    /**
     * @param n Node to check limit.
     * @param nodeName Node name.
     */
    private void checkIntegerLimit(SqlNode n, String nodeName) {
        if (n instanceof SqlLiteral) {
            BigDecimal offFetchLimit = ((SqlLiteral)n).bigDecimalValue();

            if (offFetchLimit.compareTo(DEC_INT_MAX) > 0 || offFetchLimit.compareTo(BigDecimal.ZERO) < 0)
                throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
        }
        else if (n instanceof SqlDynamicParam) {
            // will fail in params check.
            if (F.isEmpty(parameters))
                return;

            int idx = ((SqlDynamicParam)n).getIndex();

            if (idx < parameters.length) {
                Object param = parameters[idx];
                if (parameters[idx] instanceof Integer) {
                    if ((Integer)param < 0)
                        throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void validateCall(SqlCall call, SqlValidatorScope scope) {
        if (call.getKind() == SqlKind.AS) {
            final String alias = deriveAlias(call, 0);

            if (isSystemFieldName(alias))
                throw newValidationError(call, IgniteResource.INSTANCE.illegalAlias(alias));
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
    @Override protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
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

        return super.performUnconditionalRewrites(node, underFrom);
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
    @Override protected void addToSelectList(List<SqlNode> list, Set<String> aliases,
        List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
        if (includeSystemVars || exp.getKind() != SqlKind.IDENTIFIER || !isSystemFieldName(deriveAlias(exp, 0)))
            super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemField(RelDataTypeField field) {
        return isSystemFieldName(field.getName());
    }

    /** */
    private void validateAggregateFunction(SqlCall call, SqlAggFunction aggFunction) {
        if (!SqlKind.AGGREGATE.contains(aggFunction.kind))
            throw newValidationError(call,
                IgniteResource.INSTANCE.unsupportedAggregationFunction(aggFunction.getName()));

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

    /** */
    private boolean isSystemFieldName(String alias) {
        return QueryUtils.KEY_FIELD_NAME.equalsIgnoreCase(alias)
            || QueryUtils.VAL_FIELD_NAME.equalsIgnoreCase(alias);
    }

    /** {@inheritDoc} */
    @Override protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlDynamicParam && inferredType.equals(unknownType)) {
            if (parameters.length > ((SqlDynamicParam)node).getIndex()) {
                Object param = parameters[((SqlDynamicParam)node).getIndex()];

                setValidatedNodeType(node, (param == null) ? typeFactory().createSqlType(SqlTypeName.NULL) :
                    typeFactory().toSql(typeFactory().createType(param.getClass())));
            }
            else
                setValidatedNodeType(node, typeFactory().createCustomType(Object.class));
        }
        else if (node instanceof SqlCall) {
            final SqlValidatorScope newScope = scopes.get(node);

            if (newScope != null)
                scope = newScope;

            final SqlCall call = (SqlCall)node;
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
}
