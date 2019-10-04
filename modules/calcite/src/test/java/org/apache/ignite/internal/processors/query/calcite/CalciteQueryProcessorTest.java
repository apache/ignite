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

package org.apache.ignite.internal.processors.query.calcite;


import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rule.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.rule.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "true")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {

    private static CalciteQueryProcessor proc;

    @BeforeClass
    public static void setupClass() {
        proc = new CalciteQueryProcessor();

        proc.setLogger(log);
        proc.start(new GridTestKernalContext(log));

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("Developer", new IgniteTable("Developer", "Developer", (f) -> {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(f);

            builder.add(KEY_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add(VAL_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add("id", f.createJavaType(Integer.class));
            builder.add("name", f.createJavaType(String.class));
            builder.add("projectId", f.createJavaType(Integer.class));
            builder.add("cityId", f.createJavaType(Integer.class));

            return builder.build();
        }, null));

        publicSchema.addTable("Project", new IgniteTable("Project", "Project", (f) -> {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(f);

            builder.add(KEY_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add(VAL_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add("id", f.createJavaType(Integer.class));
            builder.add("name", f.createJavaType(String.class));
            builder.add("ver", f.createJavaType(Integer.class));

            return builder.build();
        }, null));

        publicSchema.addTable("Country", new IgniteTable("Country", "Country", (f) -> {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(f);

            builder.add(KEY_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add(VAL_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add("id", f.createJavaType(Integer.class));
            builder.add("name", f.createJavaType(String.class));
            builder.add("countryCode", f.createJavaType(Integer.class));

            return builder.build();
        }, null));

        publicSchema.addTable("City", new IgniteTable("City", "City", (f) -> {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(f);

            builder.add(KEY_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add(VAL_FIELD_NAME, f.createJavaType(Integer.class));
            builder.add("id", f.createJavaType(Integer.class));
            builder.add("name", f.createJavaType(String.class));
            builder.add("countryId", f.createJavaType(Integer.class));

            return builder.build();
        }, null));

        SchemaPlus schema = Frameworks.createRootSchema(false);

        schema.add("PUBLIC", publicSchema);

        proc.schemaHolder().schema(schema);
    }

    @Test
    public void testLogicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.empty(), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            IgniteDistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.LOGICAL_CONVENTION)
                .replace(IgniteDistributions.single(ImmutableIntList.of()))
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);
    }
}