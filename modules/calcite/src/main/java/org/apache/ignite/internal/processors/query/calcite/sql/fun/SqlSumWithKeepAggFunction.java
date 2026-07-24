/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
package org.apache.ignite.internal.processors.query.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Optionality;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Aggregate that sums values having the first or last ordering key.
 *
 * <p>The syntax is
 * {@code SUM_WITH_KEEP(value, 'FIRST'|'LAST') WITHIN GROUP (ORDER BY orderKey [, ...])}.
 */
public class SqlSumWithKeepAggFunction extends SqlAggFunction {
    /** */
    public SqlSumWithKeepAggFunction() {
        super(
            "SUM_WITH_KEEP",
            null,
            SqlKind.SUM,
            ReturnTypes.AGG_SUM,
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER),
            SqlFunctionCategory.NUMERIC,
            false,
            false,
            Optionality.MANDATORY
        );
    }

    /** {@inheritDoc} */
    @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (!super.checkOperandTypes(callBinding, throwOnFailure))
            return false;

        if (!callBinding.isOperandLiteral(1, false)) {
            if (throwOnFailure)
                throw callBinding.newError(RESOURCE.argumentMustBeLiteral(getName()));

            return false;
        }

        String mode = callBinding.getStringLiteralOperand(1);

        if (!"FIRST".equalsIgnoreCase(mode) && !"LAST".equalsIgnoreCase(mode)) {
            if (throwOnFailure)
                throw callBinding.newError(IgniteResource.INSTANCE.illegalSumWithKeepMode(mode));

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public Optionality getDistinctOptionality() {
        return Optionality.FORBIDDEN;
    }
}
