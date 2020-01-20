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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.ignite.internal.processors.query.calcite.type.SystemType;

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
    public IgniteSqlValidator(SqlOperatorTable opTab,
        CalciteCatalogReader catalogReader, JavaTypeFactory typeFactory,
        SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    /** {@inheritDoc} */
    @Override protected RelDataType getLogicalSourceRowType(
        RelDataType sourceRowType, SqlInsert insert) {
        return typeFactory().toSql(super.getLogicalSourceRowType(sourceRowType, insert));
    }

    /** {@inheritDoc} */
    @Override protected RelDataType getLogicalTargetRowType(
        RelDataType targetRowType, SqlInsert insert) {
        return typeFactory().toSql(super.getLogicalTargetRowType(targetRowType, insert));
    }

    /** {@inheritDoc} */
    @Override protected void addToSelectList(List<SqlNode> list, Set<String> aliases,
        List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
        if (includeSystemVars || !isSystemType(deriveType(scope, exp)))
            super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemField(RelDataTypeField field) {
        return isSystemType(field.getType());
    }

    /** */
    private boolean isSystemType(RelDataType type) {
        return type instanceof SystemType;
    }

    /** */
    private JavaTypeFactory typeFactory() {
        return (JavaTypeFactory) typeFactory;
    }
}
