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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

/** Normalizes recursive CTEs before the validator registers WITH scopes. */
class RecursiveCteRewriter {
    /** FROM operators whose first operand is a table reference. */
    private static final Set<SqlKind> FROM_WRAPPERS = EnumSet.of(
        SqlKind.AS, SqlKind.TABLE_REF, SqlKind.EXTEND, SqlKind.SNAPSHOT, SqlKind.TABLESAMPLE,
        SqlKind.LATERAL, SqlKind.PIVOT, SqlKind.UNPIVOT, SqlKind.MATCH_RECOGNIZE
    );

    /**
     * Ignores sorting of the recursive UNION before Calcite wraps it in a SELECT, hiding the recursive scope.
     * Row limiting cannot be discarded because it changes the result. Ordinary CTEs retain their ordering.
     */
    static void rewriteOrderBy(SqlWithItem item) {
        if (!(item.query instanceof SqlOrderBy) || !hasRecursiveReference(item))
            return;

        SqlOrderBy orderBy = (SqlOrderBy)item.query;

        if (orderBy.orderList.isEmpty())
            return;

        if (orderBy.fetch != null || orderBy.offset != null) {
            throw new IgniteSQLException(
                "Unsupported recursive CTE: ORDER BY with FETCH, LIMIT or OFFSET is not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION
            );
        }

        item.query = orderBy.query;
    }

    /** Finds a self-reference in the recursive operand before ORDER BY has been rewritten. */
    private static boolean hasRecursiveReference(SqlWithItem item) {
        SqlNode qry = withoutOrderBy(item.query);

        return qry.getKind() == SqlKind.UNION && references(((SqlCall)qry).operand(1), item.name, false);
    }

    /** Returns the query expression inside an optional ORDER BY wrapper. */
    private static SqlNode withoutOrderBy(SqlNode qry) {
        return qry instanceof SqlOrderBy ? ((SqlOrderBy)qry).query : qry;
    }

    /**
     * Called from the validator's existing bottom-up rewrite, so nested WITH items have already been processed.
     * Only UNION can define a recursive CTE in Calcite. Other CTEs and explicitly recursive items need no scan.
     * The seed uses the enclosing scope; only a reference in the right UNION operand can refer to this item.
     */
    static void inferRecursion(SqlWithItem item) {
        if (!item.recursive.booleanValue() && item.query.getKind() == SqlKind.UNION
            && references(((SqlCall)item.query).operand(1), item.name, false))
            item.recursive = SqlLiteral.createBoolean(true, item.recursive.getParserPosition());
    }

    /** Finds the first unqualified table reference, respecting nested WITH scopes and ignoring column names. */
    private static boolean references(SqlNode node, SqlIdentifier name, boolean from) {
        if (node == null)
            return false;

        if (node instanceof SqlIdentifier)
            // Match Calcite's WithRecursiveScope: parser casing is already applied, including quoted identifiers.
            return from && ((SqlIdentifier)node).names.equals(name.names);

        if (node instanceof SqlWith) {
            SqlWith with = (SqlWith)node;

            for (SqlNode withNode : with.withList) {
                SqlWithItem item = (SqlWithItem)withNode;
                boolean shadows = item.name.names.equals(name.names);

                SqlNode qry = withoutOrderBy(item.query);

                // ORDER BY normalization runs before nested items have had their recursive flags inferred.
                // A recursive item shadows the outer name in its recursive term, but not in its seed.
                SqlNode visibleQry = shadows && qry.getKind() == SqlKind.UNION
                    && (item.recursive.booleanValue() || hasRecursiveReference(item))
                    ? ((SqlCall)qry).operand(0) : item.query;

                if (references(visibleQry, name, false))
                    return true;

                // This item is visible in subsequent items and in the WITH body.
                if (shadows)
                    return false;
            }

            return references(with.body, name, false);
        }

        if (node instanceof SqlJoin) {
            SqlJoin join = (SqlJoin)node;

            return references(join.getLeft(), name, true)
                || references(join.getRight(), name, true)
                || references(join.getCondition(), name, false);
        }

        List<SqlNode> operands;

        if (node instanceof SqlCall)
            operands = ((SqlCall)node).getOperandList();
        else if (node instanceof SqlNodeList)
            operands = (SqlNodeList)node;
        else
            return false;

        for (int i = 0; i < operands.size(); i++) {
            SqlNode operand = operands.get(i);
            boolean childFrom = node instanceof SqlSelect
                ? operand == ((SqlSelect)node).getFrom()
                : from && i == 0 && FROM_WRAPPERS.contains(node.getKind());

            if (references(operand, name, childFrom))
                return true;
        }

        return false;
    }
}
