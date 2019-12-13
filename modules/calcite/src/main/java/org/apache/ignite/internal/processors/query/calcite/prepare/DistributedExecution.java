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

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;

/**
 *
 */
public class DistributedExecution implements QueryExecution {
    /** */
    private final PlannerContext ctx;

    /**
     * @param ctx Query context.
     */
    public DistributedExecution(PlannerContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> execute() {
        CalciteQueryProcessor proc = ctx.queryProcessor();
        Query query = ctx.query();

        RelTraitDef[] traitDefs = {
            RelDistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)) {
            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getTraitSet()
                .replace(relRoot.collation)
                .replace(IgniteConvention.INSTANCE)
                .replace(RelDistributions.ANY)
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        } catch (SqlParseException | ValidationException e) {
            String msg = "Failed to parse query.";

            proc.log().error(msg, e);

            throw new IgniteSQLException(msg, IgniteQueryErrorCode.PARSING, e);
        } catch (Exception e) {
            String msg = "Failed to create query execution graph.";

            proc.log().error(msg, e);

            throw new IgniteSQLException(msg, IgniteQueryErrorCode.UNKNOWN, e);
        }

        // TODO physical plan.

        return new ListFieldsQueryCursor<>(relRoot.rel.getRowType(), Linq4j.emptyEnumerable(), Arrays::asList);
    }
}
