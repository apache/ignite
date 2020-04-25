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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.UUID;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.Expression;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ExpressionEvaluationTest extends GridCommonAbstractTest {
    /** */
    private static final PlanningContext PLANNING_CONTEXT = PlanningContext.EMPTY;

    /** */
    private static final QueryTaskExecutor THROWING_EXECUTOR = (qid, fId, t) -> {
        throw new AssertionError();
    };

    /** */
    public static final IgniteTypeFactory TYPE_FACTORY = PLANNING_CONTEXT.typeFactory();

    @Test
    public void testCallEvaluation() throws Exception {

        RexBuilder builder = new RexBuilder(TYPE_FACTORY);

        RexNode call = builder.makeCall(
            SqlStdOperatorTable.PLUS,
            builder.makeInputRef(TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true), 0),
            builder.makeInputRef(TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false), 1));

        RexToExpTranslator translator = new RexToExpTranslator(TYPE_FACTORY);

        Expression expression = translator.translate(call);

        ExecutionContext ctx = context();

        assertEquals(3, expression.<Object>evaluate(ctx, 1, 2));
        assertEquals(4, expression.<Object>evaluate(ctx, 2, 2));
        assertEquals(1, expression.<Object>evaluate(ctx, -1, 2));
        assertEquals(null, expression.<Object>evaluate(ctx, null, 2));
    }

    public ExecutionContext context(Object... params) {
        return new ExecutionContext(THROWING_EXECUTOR, PLANNING_CONTEXT, UUID.randomUUID(), null, Commons.parametersMap(params));
    }
}
