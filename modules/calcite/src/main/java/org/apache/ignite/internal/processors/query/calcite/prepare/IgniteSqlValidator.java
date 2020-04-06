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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;

import static org.apache.calcite.util.Static.RESOURCE;

/** Validator. */
public class IgniteSqlValidator extends SqlValidatorImpl {
    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param conformance   Compatibility mode
     */
    public IgniteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader,
        IgniteTypeFactory typeFactory, SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    /** {@inheritDoc} */
    @Override public void validateInsert(SqlInsert insert) {
        if (insert.getTargetColumnList() == null)
            insert.setOperand(3, inferColumnList(insert));

        super.validateInsert(insert);
    }

    /** {@inheritDoc} */
    @Override public void validateUpdate(SqlUpdate call) {
        validateUpdateFields(call);

        super.validateUpdate(call);
    }

    /** {@inheritDoc} */
    @Override protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
        final SqlNodeList selectList = SqlNodeList.of(
            new SqlIdentifier(QueryUtils.KEY_FIELD_NAME, SqlParserPos.ZERO),
            new SqlIdentifier(QueryUtils.VAL_FIELD_NAME, SqlParserPos.ZERO));

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
    @Override public void validateCall(SqlCall call, SqlValidatorScope scope) {
        if (call.getKind() == SqlKind.AS) {
            final String alias = deriveAlias(call, 0);

            if (isSystemFieldName(alias))
                throw newValidationError(call, IgniteResource.INSTANCE.illegalAlias(alias));
        }

        super.validateCall(call, scope);
    }

    /** {@inheritDoc} */
    @Override public void validateAggregateParams(SqlCall aggCall, SqlNode filter, SqlNodeList orderList, SqlValidatorScope scope) {
        validateAggregateFunction(aggCall, (SqlAggFunction) aggCall.getOperator());

        super.validateAggregateParams(aggCall, filter, orderList, scope);
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
            case SUM:
            case AVG:
            case MIN:
            case MAX:

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

        final TableDescriptor desc = table.unwrap(TableDescriptor.class);

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

        final TableDescriptor desc = table.unwrap(TableDescriptor.class);

        if (desc == null)
            return;

        final RelDataType baseType = table.getRowType();
        final RelOptTable relOptTable = relOptTable(ns);

        for (SqlNode node : call.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier) node;

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
        return (IgniteTypeFactory) typeFactory;
    }

    /** */
    private boolean isSystemFieldName(String alias) {
        return QueryUtils.KEY_FIELD_NAME.equalsIgnoreCase(alias)
            || QueryUtils.VAL_FIELD_NAME.equalsIgnoreCase(alias);
    }
}
