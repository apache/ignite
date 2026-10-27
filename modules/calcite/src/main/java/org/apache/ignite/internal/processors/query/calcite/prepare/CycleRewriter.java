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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlWithItemTableRef;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlWithItem;
import org.apache.ignite.internal.processors.query.calcite.sql.SqlCycleClause;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.validate.SqlValidatorUtil.EXPR_SUGGESTER;
import static org.apache.calcite.util.Static.RESOURCE;

/** Lowers CYCLE to a recursive UNION with a mark and an array of key tuples.
 *
 * <p>Each row has its own path. The row closing a cycle is emitted, but is
 * filtered out before evaluating the next recursive step. Fresh syntax nodes
 * are used because the original query has already been validated. */
final class CycleRewriter {
    // TODO: https://issues.apache.org/jira/browse/IGNITE-29207 Remove after upgrading to Calcite 1.43.
    // Ported from CALCITE-7814 (8f27e24a02e7b76fc40b6cfd9b1d2cc96bc8b1b6).

    /** Position for generated syntax nodes. */
    private static final SqlParserPos POS = SqlParserPos.ZERO;

    /** Alias for the original projection inside each rewritten UNION operand. */
    private static final String INPUT = "$CYCLE_INPUT";

    /** Validator holding the original scopes, expanded projections and types. */
    private final IgniteSqlValidator validator;

    /** Original WITH item, before generated columns are appended. */
    private final SqlWithItem item;

    /** Parsed cycle specification. */
    private final SqlCycleClause cycle;

    /** Identifier matching rules of the current catalog. */
    private final SqlNameMatcher matcher;

    /** */
    private CycleRewriter(IgniteSqlValidator validator, SqlWithItem item) {
        this.validator = validator;
        this.item = item;
        this.cycle = requireNonNull(((IgniteSqlWithItem)item).cycleClause);
        this.matcher = validator.getCatalogReader().nameMatcher();
    }

    /** */
    static SqlWithItem rewrite(IgniteSqlValidator validator, SqlValidatorScope scope,
            SqlWithItem item) {
        return new CycleRewriter(validator, item).rewrite(scope);
    }

    /** Replaces CYCLE with explicit mark and path columns in both UNION operands.
     *
     * <p>For {@code CYCLE k1, k2 SET mark TO cycleValue DEFAULT defaultValue
     * USING path}, the transformation is schematically:
     *
     * <pre>{@code
     * seed output:
     *   original columns,
     *   defaultValue AS mark,
     *   ARRAY[ROW(k1, k2)] AS path
     * recursive branch:
     *   SELECT original output columns,
     *          CASE WHEN contains(previousPath, ROW(k1, k2))
     *               THEN cycleValue ELSE defaultValue END AS mark,
     *          append(previousPath, ROW(k1, k2)) AS path
     *   FROM (
     *     SELECT original projection, previous.path AS previousPath
     *     FROM original sources
     *     WHERE (original condition)
     *       AND previous.mark <> cycleValue
     *   ) AS step
     * }</pre>
     *
     * <p>Here {@code previous} is the recursive CTE reference in the original
     * FROM clause; the original condition is TRUE if there was no WHERE clause.
     * The added WHERE condition stops expanding rows already marked as cycles.
     * The recursive output keys are taken after the user's projection. A row
     * closing a cycle is therefore returned with its extended path, but cannot
     * feed the next recursive step. The original UNION [ALL] is retained.
     * The returned WITH item declares the two additional columns and no longer
     * has a CYCLE clause. */
    private SqlWithItem rewrite(SqlValidatorScope scope) {
        final Reference ref = validateRecursiveQuery();
        final RelDataType rowType = requireNonNull(validator.getNamespace(item)).getRowType();
        final List<RelDataTypeField> keys = validateKeys(rowType);
        final List<String> names = rowType.getFieldNames();
        validateGeneratedNames(names);
        final RelDataType markType = validateMarkType(scope);

        final SqlNode qry = prepareQuery(ref);
        final SqlCall union = (SqlCall)queryBody(qry);
        union.setOperand(0, rewriteSeed(union.operand(0), names, keys, markType));
        union.setOperand(1, rewriteStep(union.operand(1), ref, names, keys, markType));

        final List<SqlNode> outputNames = identifiers(names);
        outputNames.add(copy(cycle.markColumn));
        outputNames.add(copy(cycle.pathColumn));
        return new SqlWithItem(item.getParserPosition(), item.name,
                new SqlNodeList(outputNames, POS), qry, item.recursive);
    }

    /** Checks the recursive query's shape without changing its syntax tree. */
    private Reference validateRecursiveQuery() {
        final SqlNode body = queryBody(item.query);
        if (body.getKind() != SqlKind.UNION
                || !(((SqlCall)body).operand(1) instanceof SqlSelect)) {
            throw validator.newValidationError(cycle, IgniteResource.INSTANCE.cycleRequiresRecursiveSelect());
        }
        final SqlSelect step = ((SqlCall)body).operand(1);
        final List<SqlNode> references = new ArrayList<>();
        findDirectReferences(step.getFrom(), references);
        if (references.size() != 1 || recursiveReferenceCount() != 1) {
            throw validator.newValidationError(cycle, IgniteResource.INSTANCE.cycleRequiresRecursiveSelect());
        }
        if (validator.isAggregate(step) || step.isDistinct() || !step.getWindowList().isEmpty()
                || SqlUtil.containsCall(step, call -> call.getKind() == SqlKind.OVER)) {
            throw validator.newValidationError(cycle, IgniteResource.INSTANCE.cycleRecursiveSelectNotSupported());
        }
        return reference(references.get(0));
    }

    /** Finds direct references on the non-null-generating sides of joins. */
    private void findDirectReferences(@Nullable SqlNode from, List<SqlNode> references) {
        if (isSelfReference(from)) {
            references.add(from);
        }
        else if (from instanceof SqlJoin) {
            final SqlJoin join = (SqlJoin)from;
            if (!join.getJoinType().generatesNullsOnLeft()) {
                findDirectReferences(join.getLeft(), references);
            }
            if (!join.getJoinType().generatesNullsOnRight()) {
                findDirectReferences(join.getRight(), references);
            }
        }
        else if (from != null && from.getKind() == SqlKind.AS
                && isSelfReference(((SqlCall)from).operand(0))) {
            references.add(from);
        }
    }

    /** */
    private boolean isSelfReference(@Nullable SqlNode node) {
        return node instanceof SqlIdentifier && ((SqlIdentifier)node).isSimple()
                && matcher.matches(((SqlIdentifier)node).getSimple(), item.name.getSimple());
    }

    /** Counts resolved references as well, to catch references hidden in subqueries. */
    private int recursiveReferenceCount() {
        final List<SqlIdentifier> references = new ArrayList<>();
        item.query.accept(new SqlBasicVisitor<Void>() {
            @Override public Void visit(SqlIdentifier id) {
                final SqlValidatorNamespace ns = validator.getNamespace(id);
                if (ns != null) {
                    final SqlNode node = ns.resolve().getNode();
                    if (node instanceof SqlWithItemTableRef
                            && ((SqlWithItemTableRef)node).getWithItem() == item) {
                        references.add(id);
                    }
                }
                return null;
            }
        });
        return references.size();
    }

    /** */
    private List<RelDataTypeField> validateKeys(RelDataType rowType) {
        final List<RelDataTypeField> keys = new ArrayList<>();
        for (SqlNode node : cycle.columns) {
            final SqlIdentifier key = (SqlIdentifier)node;
            final RelDataTypeField field = matcher.field(rowType, key.getSimple());
            if (field == null) {
                throw validator.newValidationError(key,
                        IgniteResource.INSTANCE.cycleColumnNotFound(key.getSimple(), item.name.getSimple()));
            }
            if (keys.contains(field)) {
                throw validator.newValidationError(key,
                        RESOURCE.duplicateNameInColumnList(key.getSimple()));
            }
            keys.add(field);
        }
        return keys;
    }

    /** */
    private void validateGeneratedNames(List<String> names) {
        if (matcher.matches(cycle.markColumn.getSimple(), cycle.pathColumn.getSimple())) {
            throw validator.newValidationError(cycle.pathColumn,
                    IgniteResource.INSTANCE.cycleColumnConflict(cycle.pathColumn.getSimple()));
        }
        for (SqlIdentifier generated : ImmutableList.of(cycle.markColumn, cycle.pathColumn)) {
            if (matcher.indexOf(names, generated.getSimple()) >= 0) {
                throw validator.newValidationError(generated,
                        IgniteResource.INSTANCE.cycleColumnConflict(generated.getSimple()));
            }
        }
    }

    /** */
    private RelDataType validateMarkType(SqlValidatorScope scope) {
        if (!(cycle.markValue instanceof SqlLiteral)
                || !(cycle.defaultValue instanceof SqlLiteral)) {
            throw validator.newValidationError(cycle, IgniteResource.INSTANCE.cycleInvalidMarkValues());
        }
        final Object mark = ((SqlLiteral)cycle.markValue).getValue();
        final Object dfltMark = ((SqlLiteral)cycle.defaultValue).getValue();
        final boolean valid;
        if (mark instanceof Boolean && dfltMark instanceof Boolean) {
            valid = !mark.equals(dfltMark);
        }
        else if (mark instanceof NlsString && dfltMark instanceof NlsString) {
            final String markStr = ((NlsString)mark).getValue();
            final String dfltStr = ((NlsString)dfltMark).getValue();
            valid = markStr.length() == 1 && dfltStr.length() == 1
                    && !markStr.equals(dfltStr);
        }
        else {
            valid = false;
        }
        if (!valid) {
            throw validator.newValidationError(cycle, IgniteResource.INSTANCE.cycleInvalidMarkValues());
        }
        final RelDataType type =
                validator.getTypeFactory().leastRestrictive(
                        ImmutableList.of(validator.deriveType(scope, cycle.markValue),
                                validator.deriveType(scope, cycle.defaultValue)));
        if (type == null) {
            throw validator.newValidationError(cycle, IgniteResource.INSTANCE.cycleInvalidMarkValues());
        }
        return type;
    }

    /** Chooses generated column names, accounting for an explicit alias column list. */
    private Reference reference(SqlNode source) {
        final SqlCall as = source instanceof SqlCall ? (SqlCall)source : null;
        final String alias = requireNonNull(SqlValidatorUtil.alias(source), "alias");
        final Set<String> aliases = matcher.createSet();
        if (as != null && as.operandCount() > 2) {
            aliases.addAll(
                    SqlIdentifier.simpleNames(as.getOperandList().subList(2, as.operandCount())));
        }
        final String mark =
                SqlValidatorUtil.uniquify(cycle.markColumn.getSimple(), aliases, EXPR_SUGGESTER);
        final String path =
                SqlValidatorUtil.uniquify(cycle.pathColumn.getSimple(), aliases, EXPR_SUGGESTER);
        return new Reference(source, qualified(alias, mark), qualified(alias, path));
    }

    /** Copies the query and adjusts joins and aliases before adding generated columns. */
    private SqlNode prepareQuery(Reference reference) {
        final SqlSelect step = ((SqlCall)queryBody(item.query)).operand(1);
        final SqlNodeList stepSelectList = step.getSelectList();
        final SelectScope stepScope = requireNonNull(validator.getRawSelectScope(step));
        // Validation saves the expanded projection even if identifier expansion is disabled.
        final SqlNodeList expandedSelectList =
                new SqlNodeList(requireNonNull(stepScope.getExpandedSelectList()),
                        stepSelectList.getParserPosition());
        final SqlShuttle shuttle = new DeepCopier() {
            @Override public SqlNode visit(SqlNodeList nodeList) {
                return requireNonNull(
                        super.visit(nodeList == stepSelectList ? expandedSelectList : nodeList));
            }

            @Override public SqlNode visit(SqlCall call) {
                final SqlCall result = (SqlCall)super.visit(call);
                if (call instanceof SqlJoin && ((SqlJoin)call).isNatural()) {
                    makeJoinExplicit((SqlJoin)call, (SqlJoin)result);
                }
                if (call == reference.source && call.operandCount() > 2) {
                    final List<SqlNode> operands = new ArrayList<>(result.getOperandList());
                    operands.add(new SqlIdentifier(Util.last(reference.mark.names), POS));
                    operands.add(new SqlIdentifier(Util.last(reference.path.names), POS));
                    return result.getOperator().createCall(result.getParserPosition(), operands);
                }
                return result;
            }
        };
        return requireNonNull(item.query.accept(shuttle));
    }

    /** */
    private void makeJoinExplicit(SqlJoin original, SqlJoin copy) {
        final List<String> names = requireNonNull(validator.usingNames(original));
        copy.setOperand(1, SqlLiteral.createBoolean(false, POS));
        copy.setOperand(4,
                (names.isEmpty() ? JoinConditionType.ON : JoinConditionType.USING).symbol(POS));
        copy.setOperand(5, names.isEmpty() ? SqlLiteral.createBoolean(true, POS)
                : new SqlNodeList(identifiers(names), POS));
    }

    /** */
    private SqlSelect rewriteSeed(SqlNode seed, List<String> names,
            List<RelDataTypeField> keys, RelDataType markType) {
        final List<SqlNode> columns = columns(names);
        columns.add(cast(copy(cycle.defaultValue), markType));
        columns.add(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(POS, key(keys)));
        return select(columns, alias(seed, names));
    }

    /** */
    private SqlSelect rewriteStep(SqlSelect step, Reference reference, List<String> names,
            List<RelDataTypeField> keys, RelDataType markType) {
        excludeCompletedPaths(step, reference, markType);
        final List<String> inputNames = new ArrayList<>(names);
        inputNames.add(cycle.pathColumn.getSimple());
        final List<SqlNode> columns = columns(names);
        columns.add(cycleMark(keys, markType));
        columns.add(
                IgniteOwnSqlOperatorTable.CYCLE_PATH_APPEND.createCall(POS, previousPath(), key(keys)));
        return select(columns, alias(step, inputNames));
    }

    /** Filters closed paths and carries the previous path through the user's projection. */
    private void excludeCompletedPaths(SqlSelect step, Reference reference, RelDataType markType) {
        final SqlNode mark = cast(copy(cycle.markValue), markType);
        final SqlNode live =
                SqlStdOperatorTable.NOT_EQUALS.createCall(POS, reference.mark, mark);
        step.setWhere(step.getWhere() == null ? live
                : SqlStdOperatorTable.AND.createCall(POS, step.getWhere(), live));
        final List<SqlNode> columns = new ArrayList<>(step.getSelectList());
        columns.add(reference.path);
        step.setSelectList(new SqlNodeList(columns, POS));
    }

    /** */
    private SqlNode cycleMark(List<RelDataTypeField> keys, RelDataType markType) {
        final SqlNode contains =
                IgniteOwnSqlOperatorTable.CYCLE_PATH_CONTAINS.createCall(POS, previousPath(), key(keys));
        return new SqlCase(POS, null, SqlNodeList.of(contains),
                SqlNodeList.of(cast(copy(cycle.markValue), markType)),
                cast(copy(cycle.defaultValue), markType));
    }

    /** */
    private SqlIdentifier previousPath() {
        return qualified(INPUT, cycle.pathColumn.getSimple());
    }

    /** */
    private static SqlNode queryBody(SqlNode query) {
        while (query instanceof SqlWith) {
            query = ((SqlWith)query).body;
        }
        return query;
    }

    /** Builds key tuples with the original SQL types and field names retained in the path. */
    private static SqlNode key(List<RelDataTypeField> keys) {
        // Calcite 1.42 lacks SqlRowOperator's fieldNames constructor. Names are needed only during validation;
        // IgniteConvertletTable preserves the resulting type and normalizes the operator for plan serialization.
        return new SqlRowOperator("ROW") {
            /** {@inheritDoc} */
            @Override public RelDataType inferReturnType(SqlOperatorBinding binding) {
                RelDataType type = super.inferReturnType(binding);
                RelDataTypeFactory.Builder builder = binding.getTypeFactory().builder();

                for (int i = 0; i < keys.size(); i++)
                    builder.add(keys.get(i).getName(), type.getFieldList().get(i).getType());

                return binding.getTypeFactory().createTypeWithNullability(builder.build(), type.isNullable());
            }

            /** {@inheritDoc} */
            @Override public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
                for (SqlNode operand : call.getOperandList())
                    validator.deriveType(scope, operand);

                return validateOperands(validator, scope, call);
            }
        }.createCall(POS, Util.transform(keys, field -> cast(qualified(INPUT, field.getName()), field.getType())));
    }

    /** */
    private static SqlNode cast(SqlNode value, RelDataType type) {
        final SqlNode typeSpec = SqlTypeUtil.isInterval(type)
                ? SqlNode.clone(type.getIntervalQualifier())
                : SqlTypeUtil.convertTypeToSpec(type);
        return SqlStdOperatorTable.CAST.createCall(POS, value, typeSpec);
    }

    /** */
    private static SqlIdentifier qualified(String table, String column) {
        return new SqlIdentifier(ImmutableList.of(table, column), POS);
    }

    /** */
    private static List<SqlNode> columns(List<String> names) {
        return new ArrayList<>(Util.transform(names, name -> qualified(INPUT, name)));
    }

    /** */
    private static List<SqlNode> identifiers(List<String> names) {
        return new ArrayList<>(Util.transform(names, name -> new SqlIdentifier(name, POS)));
    }

    /** */
    private static SqlNode alias(SqlNode query, List<String> names) {
        final List<SqlNode> operands = new ArrayList<>();
        operands.add(query);
        operands.add(new SqlIdentifier(INPUT, POS));
        operands.addAll(identifiers(names));
        return SqlStdOperatorTable.AS.createCall(POS, operands);
    }

    /** */
    private static SqlSelect select(List<SqlNode> columns, SqlNode from) {
        return new SqlSelect(POS, null, new SqlNodeList(columns, POS), from,
                null, null, null, null, null, null, null, null, null);
    }

    /** */
    private static SqlNode copy(SqlNode node) {
        return requireNonNull(node.accept(new DeepCopier()));
    }

    /** Original recursive table reference and its qualified generated columns. */
    private static class Reference {

        /** */
        final SqlNode source;

        /** */
        final SqlIdentifier mark;

        /** */
        final SqlIdentifier path;

        /** */
        Reference(SqlNode source, SqlIdentifier mark, SqlIdentifier path) {
            this.source = source;
            this.mark = mark;
            this.path = path;
        }
    }

    /** Copies syntax nodes without reusing the validator's cached call types. */
    private static class DeepCopier extends SqlShuttle {

        /** */
        @Override public SqlNode visit(SqlIdentifier id) {
            return SqlNode.clone(id);
        }

        /** */
        @Override public SqlNode visit(SqlLiteral literal) {
            return SqlNode.clone(literal);
        }

        /** */
        @Override public SqlNode visit(SqlCall call) {
            final CallCopyingArgHandler hnd = new CallCopyingArgHandler(call, true);
            call.getOperator().acceptCall(this, call, false, hnd);
            return hnd.result();
        }
    }
}
