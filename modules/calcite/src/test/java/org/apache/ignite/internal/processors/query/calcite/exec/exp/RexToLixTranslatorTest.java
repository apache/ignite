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

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link RexToLixTranslator}. */
public class RexToLixTranslatorTest {
    /** */
    @Test
    public void testEmptyStringResultIsNormalizedOnce() {
        IgniteTypeFactory typeFactory = new IgniteTypeFactory();
        IgniteRexBuilder rexBuilder = new IgniteRexBuilder(typeFactory, true);

        RelDataType strType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType rowType = typeFactory.builder().add("VAL", strType).build();

        RexNode input = rexBuilder.makeInputRef(strType, 0);
        RexNode upper = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, input);
        RexNode lower = rexBuilder.makeCall(SqlStdOperatorTable.LOWER, upper);

        RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, rexBuilder);
        programBuilder.addProject(lower, "RES");
        RexProgram program = programBuilder.getProgram();

        BlockBuilder block = new BlockBuilder();
        ParameterExpression inputVal = Expressions.parameter(String.class, "input");

        List<Expression> projects = RexToLixTranslator.translateProjects(
            program,
            typeFactory,
            SqlConformanceEnum.DEFAULT,
            block,
            null,
            DataContext.ROOT,
            (builder, idx, storageType) -> inputVal,
            null,
            true
        );

        block.add(Expressions.return_(null, projects.get(0)));

        String code = block.toBlock().toString();

        // One normalization for the input and one for each string function result.
        assertEquals(code, 3, occurrences(code, "nullIfEmpty("));
    }

    /** Counts non-overlapping occurrences of the specified substring. */
    private static int occurrences(String str, String substr) {
        int cnt = 0;

        for (int pos = 0; (pos = str.indexOf(substr, pos)) >= 0; pos += substr.length())
            cnt++;

        return cnt;
    }
}
