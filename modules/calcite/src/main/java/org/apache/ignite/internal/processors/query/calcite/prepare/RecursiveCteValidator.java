/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlWithItemTableRef;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

/** Validates restrictions specific to recursive common table expressions. */
final class RecursiveCteValidator {
    /** */
    private RecursiveCteValidator() {
        // No-op.
    }

    /** Validates that a recursive CTE does not reference itself from a nested query. */
    static void validate(IgniteSqlValidator validator, SqlWithItem withItem) {
        if (withItem.recursive == null || !withItem.recursive.booleanValue())
            return;

        Set<SqlNode> topLevelQueries = Collections.newSetFromMap(new IdentityHashMap<>());

        collectTopLevelQueries(withItem.query, topLevelQueries);

        RecursiveCteReferenceFinder finder =
            new RecursiveCteReferenceFinder(validator, withItem, topLevelQueries);

        withItem.query.accept(finder);

        if (finder.nestedSelfReferenceFound) {
            throw new IgniteSQLException(
                "Unsupported recursive CTE: self-references inside subqueries are not supported",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION
            );
        }
    }

    /** Collects query nodes that constitute the CTE's top-level query expression. */
    private static void collectTopLevelQueries(SqlNode node, Set<SqlNode> queries) {
        if (node == null || !node.isA(SqlKind.QUERY))
            return;

        queries.add(node);

        if (node.isA(SqlKind.SET_QUERY)) {
            for (SqlNode operand : ((SqlCall)node).getOperandList())
                collectTopLevelQueries(operand, queries);
        }
        else if (node instanceof SqlWith)
            collectTopLevelQueries(((SqlWith)node).body, queries);
        else if (node.getKind() == SqlKind.ORDER_BY)
            collectTopLevelQueries(((SqlCall)node).operand(0), queries);
    }

    /** Finds references to the recursive CTE located inside nested queries. */
    private static class RecursiveCteReferenceFinder extends SqlBasicVisitor<Void> {
        /** SQL validator. */
        private final IgniteSqlValidator validator;

        /** Recursive CTE being validated. */
        private final SqlWithItem withItem;

        /** Nodes that belong to the CTE's top-level query expression. */
        private final Set<SqlNode> topLevelQueries;

        /** Whether the visitor is inside a nested query. */
        private boolean insideSubquery;

        /** Whether an unsupported reference was found. */
        private boolean nestedSelfReferenceFound;

        /** */
        private RecursiveCteReferenceFinder(
            IgniteSqlValidator validator,
            SqlWithItem withItem,
            Set<SqlNode> topLevelQueries
        ) {
            this.validator = validator;
            this.withItem = withItem;
            this.topLevelQueries = topLevelQueries;
        }

        /** {@inheritDoc} */
        @Override public Void visit(SqlCall call) {
            if (nestedSelfReferenceFound)
                return null;

            boolean wasInsideSubquery = insideSubquery;

            insideSubquery |= call.isA(SqlKind.QUERY) && !topLevelQueries.contains(call);

            super.visit(call);

            insideSubquery = wasInsideSubquery;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void visit(SqlIdentifier id) {
            if (!insideSubquery)
                return null;

            SqlValidatorNamespace namespace = validator.getNamespace(id);
            SqlNode resolvedNode = namespace == null ? null : namespace.resolve().getNode();

            if (resolvedNode instanceof SqlWithItemTableRef
                && ((SqlWithItemTableRef)resolvedNode).getWithItem() == withItem) {
                nestedSelfReferenceFound = true;
            }

            return null;
        }
    }
}
