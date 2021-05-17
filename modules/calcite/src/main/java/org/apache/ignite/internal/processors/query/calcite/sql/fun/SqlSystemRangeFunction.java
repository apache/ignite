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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * Definition of the "SUBSTRING" builtin SQL function.
 */
public class SqlSystemRangeFunction extends SqlFunction implements SqlTableFunction {
    /**
     * Creates the SqlSystemRangeFunction.
     */
    SqlSystemRangeFunction() {
        super(
            "SYSTEM_RANGE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.CURSOR,
            null,
            OperandTypes.or(
                OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
                OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
            ),
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    /** {@inheritDoc} */
    @Override public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep(", ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        if (3 == call.operandCount()) {
            writer.sep(", ");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
        }

        writer.endFunCall(frame);
    }

    /** {@inheritDoc} */
    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.MONOTONIC;
    }

    /** {@inheritDoc} */
    @Override public SqlReturnTypeInference getRowTypeInference() {
        return cb -> cb.getTypeFactory().builder().add("X", SqlTypeName.BIGINT).build();
    }
}
