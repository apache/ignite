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

import java.util.Locale;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Definition of the "XML_TABLE" builtin SQL table function.
 */
public class SqlXmlTableFunction extends SqlFunction implements SqlTableFunction {
    /**
     * Creates the SqlXmlTableFunction.
     */
    SqlXmlTableFunction() {
        super(
            "XML_TABLE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.CURSOR,
            null,
            OperandTypes.repeat(SqlOperandCountRanges.from(4), OperandTypes.CHARACTER),
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    /** {@inheritDoc} */
    @Override public SqlReturnTypeInference getRowTypeInference() {
        return cb -> {
            int colArgsCnt = cb.getOperandCount() - 2;

            if (colArgsCnt <= 0 || colArgsCnt % 2 != 0)
                throw new IllegalArgumentException("XML_TABLE columns must be specified as path/type pairs.");

            org.apache.calcite.rel.type.RelDataTypeFactory.Builder b = cb.getTypeFactory().builder();

            for (int i = 0; i < colArgsCnt; i += 2)
                b.add("C" + (i / 2 + 1), columnType(cb.getStringLiteralOperand(i + 3)));

            return b.build();
        };
    }

    /** */
    private static SqlTypeName columnType(String type) {
        switch (type.toUpperCase(Locale.ROOT)) {
            case "STRING":
            case "VARCHAR":
                return SqlTypeName.VARCHAR;

            case "INT":
            case "INTEGER":
                return SqlTypeName.INTEGER;

            default:
                throw new IllegalArgumentException("Unsupported XML_TABLE column type: " + type);
        }
    }
}
