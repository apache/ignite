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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rule.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.rule.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.RowType;
import org.apache.ignite.internal.processors.query.calcite.splitter.PartitionsDistribution;
import org.apache.ignite.internal.processors.query.calcite.splitter.PartitionsDistributionRegistry;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {

    private static CalciteQueryProcessor proc;
    private static SchemaPlus schema;

    private static PartitionsDistribution developerDistribution;
    private static PartitionsDistribution projectDistribution;
    private static PartitionsDistribution randomDistribution;
    private static PartitionsDistribution singleDistribution;
    private static PartitionsDistributionRegistry registry;

    @BeforeClass
    public static void setupClass() {
        proc = new CalciteQueryProcessor();

        proc.setLogger(log);
        proc.start(new GridTestKernalContext(log));

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("Developer", new IgniteTable("Developer", "Developer",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("projectId", Integer.class)
                .field("cityId", Integer.class)
                .build()));

        publicSchema.addTable("Project", new IgniteTable("Project", "Project",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("ver", Integer.class)
                .build()));



        publicSchema.addTable("Country", new IgniteTable("Country", "Country",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("countryCode", Integer.class)
                .build()));

        publicSchema.addTable("City", new IgniteTable("City", "City",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("countryId", Integer.class)
                .build()));

        schema = Frameworks.createRootSchema(false);

        schema.add("PUBLIC", publicSchema);

        developerDistribution = new PartitionsDistribution();

        developerDistribution.parts = 5;
        developerDistribution.nodes = new int[]{0,1,2};
        developerDistribution.nodeParts = new int[][]{{1,2},{3,4},{5}};

        projectDistribution = new PartitionsDistribution();

        projectDistribution.excessive = true;
        projectDistribution.parts = 5;
        projectDistribution.nodes = new int[]{0,1,2};
        projectDistribution.nodeParts = new int[][]{{1,2,3,5},{2,3,4},{1,4,5}};

        randomDistribution = new PartitionsDistribution();
        randomDistribution.parts = 3;
        randomDistribution.nodes = new int[]{0,1,2};
        randomDistribution.nodeParts = new int[][]{{1},{2},{3}};

        singleDistribution = new PartitionsDistribution();
        singleDistribution.parts = 1;
        singleDistribution.nodes = new int[]{0};
        singleDistribution.nodeParts = new int[][]{{1}};

        registry = new PartitionsDistributionRegistry() {
            @Override public PartitionsDistribution get(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return developerDistribution;
                if (cacheId == CU.cacheId("Project"))
                    return projectDistribution;

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }

            @Override public PartitionsDistribution random(AffinityTopologyVersion topVer) {
                return randomDistribution;
            }

            @Override public PartitionsDistribution single() {
                return singleDistribution;
            }
        };
    }

    @Test
    public void testLogicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
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
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);
    }
}