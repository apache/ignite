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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Arrays;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class RexUtils {
    /** */
    public static RexNode makeCast(RexBuilder builder, RelDataType toType, RexNode node) {
        return TypeUtils.needCast(builder.getTypeFactory(), node.getType(), toType)
            ? builder.makeCast(toType, node)
            : node;
    }

    /** */
    public static RexBuilder builder(RelNode rel) {
        return builder(rel.getCluster());
    }

    /** */
    public static RexBuilder builder(RelOptCluster cluster) {
        return cluster.getRexBuilder();
    }

    /** */
    public static RexExecutor executor(RelNode rel) {
        return executor(rel.getCluster());
    }

    /** */
    public static RexExecutor executor(RelOptCluster cluster) {
        return Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    }

    /** */
    public static RexSimplify simplifier(RelOptCluster cluster) {
        return new RexSimplify(builder(cluster), RelOptPredicateList.EMPTY, executor(cluster));
    }

    /** */
    public static RexNode makeCase(RexBuilder builder, RexNode... operands) {
        if (U.assertionsEnabled()) {
            // each odd operand except last one has to return a boolean type
            for (int i = 0; i < operands.length; i += 2) {
                if (operands[i].getType().getSqlTypeName() != SqlTypeName.BOOLEAN && i < operands.length - 1) {
                    throw new AssertionError("Unexpected operand type. " +
                        "[operands=" + Arrays.toString(operands) + "]");
                }
            }
        }

        return builder.makeCall(SqlStdOperatorTable.CASE, operands);
    }
}
