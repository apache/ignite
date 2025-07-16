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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Ignite SQL call rewrite table. Performs unconditional rewrites for some predefined Calcite SQL operators,
 * which can't be extended other ways by Ignite.
 */
public class IgniteSqlCallRewriteTable {
    /** Instance. */
    public static final IgniteSqlCallRewriteTable INSTANCE = new IgniteSqlCallRewriteTable();

    /**
     * Registered rewriters map.
     * If there are function overloads exists, validator can't resolve SqlOperator only by name, before unconditinal
     * rewrite. SqlCall contains SqlUnresolvedFunction as an operator. We can find a rewriter for such a call only by
     * operator name (string). Rewriter should ensure that it's a correct operator to rewrite (for example,
     * additionally checking operands count) and skip operators with the unknown signature.
     */
    private final Map<String, BiFunction<SqlValidator, SqlCall, SqlCall>> map = new ConcurrentHashMap<>();

    /** */
    private IgniteSqlCallRewriteTable() {
        register(SqlLibraryOperators.NVL.getName(), IgniteSqlCallRewriteTable::nvlRewriter);
        register(SqlLibraryOperators.DECODE.getName(), IgniteSqlCallRewriteTable::decodeRewriter);

        // TODO Workaround for https://issues.apache.org/jira/browse/CALCITE-6978
        register(SqlStdOperatorTable.COALESCE.getName(), IgniteSqlCallRewriteTable::coalesceRewriter);
    }

    /** Registers rewriter for SQL operator. */
    public void register(String operatorName, BiFunction<SqlValidator, SqlCall, SqlCall> rewriter) {
        map.put(operatorName, rewriter);
    }

    /** Rewrites SQL call. */
    SqlNode rewrite(SqlValidator validator, SqlCall call) {
        BiFunction<SqlValidator, SqlCall, SqlCall> rewriter = map.get(call.getOperator().getName());

        return rewriter == null ? call.getOperator().rewriteCall(validator, call) : rewriter.apply(validator, call);
    }

    /** */
    private static SqlCall coalesceRewriter(SqlValidator validator, SqlCall call) {
        return containsSubquery(call) ? call : (SqlCall)call.getOperator().rewriteCall(validator, call);
    }

    /** Rewrites NVL call to CASE WHEN call. */
    private static SqlCall nvlRewriter(SqlValidator validator, SqlCall call) {
        validateQuantifier(validator, call); // check DISTINCT/ALL

        List<SqlNode> operands = call.getOperandList();

        if (operands.size() == 2) {
            SqlParserPos pos = call.getParserPosition();

            // Do not rewrite to CASE-WHEN calls that contain subqueries.
            if (containsSubquery(call))
                return new SqlBasicCall(SqlStdOperatorTable.COALESCE, operands, pos);

            SqlNodeList whenList = new SqlNodeList(pos);
            SqlNodeList thenList = new SqlNodeList(pos);

            whenList.add(SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, operands.get(0)));
            thenList.add(SqlNode.clone(operands.get(0)));
            SqlNode elseExpr = operands.get(1);

            return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr);
        }
        else
            return call; // Operands count will be validated and exception will be thrown later.
    }

    /** */
    public static boolean containsSubquery(SqlNode call) {
        try {
            SqlVisitor<Void> visitor = new SqlBasicVisitor<>() {
                @Override public Void visit(SqlCall call) {
                    if (call.getKind() == SqlKind.SELECT)
                        throw new Util.FoundOne(call);

                    return super.visit(call);
                }
            };

            call.accept(visitor);

            return false;
        }
        catch (Util.FoundOne e) {
            return true;
        }
    }

    /** Rewrites DECODE call to CASE WHEN call. */
    private static SqlCall decodeRewriter(SqlValidator validator, SqlCall call) {
        validateQuantifier(validator, call); // check DISTINCT/ALL

        List<SqlNode> operands = call.getOperandList();

        SqlParserPos pos = call.getParserPosition();

        SqlNode op0 = operands.get(0);

        SqlNodeList whenList = new SqlNodeList(pos);
        SqlNodeList thenList = new SqlNodeList(pos);

        for (int i = 1; i < operands.size() - 1; i += 2) {
            whenList.add(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM.createCall(pos, SqlNode.clone(op0), operands.get(i)));
            thenList.add(operands.get(i + 1));
        }

        SqlNode elseExpr = operands.size() % 2 == 0 ? operands.get(operands.size() - 1) : null;

        return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr);
    }

    /** Throws a validation error if a DISTINCT or ALL quantifier is present. */
    private static void validateQuantifier(SqlValidator validator, SqlCall call) {
        if (call.getFunctionQuantifier() != null) {
            throw validator.newValidationError(call.getFunctionQuantifier(),
                RESOURCE.functionQuantifierNotAllowed(call.getOperator().getName()));
        }
    }
}
