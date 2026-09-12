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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.Arrays;
import java.util.function.Predicate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExecutionPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryTemplate;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/** */
public class PlanSplitterTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema createSchema(
        IgniteDistribution distribution1,
        ColocationGroup colocationGrp1,
        IgniteDistribution distribution2,
        ColocationGroup colocationGrp2,
        int... sizes
    ) {
        assert sizes.length < 3;

        int sz1 = sizes.length > 0 ? sizes[0] : DEFAULT_TBL_SIZE;
        int sz2 = sizes.length > 1 ? sizes[1] : DEFAULT_TBL_SIZE;

        return createSchema(
            createTable("DEVELOPER", sz1, distribution1, "ID", INTEGER, "NAME", VARCHAR, "PROJECTID", INTEGER)
                .setColocationGroup(colocationGrp1),
            createTable("PROJECT", sz2, distribution2, "ID", INTEGER, "NAME", VARCHAR, "VER", INTEGER)
                .setColocationGroup(colocationGrp2)
        );
    }

    /** */
    @Test
    public void testSplitterColocatedPartitionedPartitioned() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.affinity(0, "Developer", "hash"),
            ColocationGroup.forAssignments(Arrays.asList(
                select(nodes, 0, 1),
                select(nodes, 1, 2),
                select(nodes, 2, 0),
                select(nodes, 0, 1),
                select(nodes, 1, 2)
            )),
            IgniteDistributions.affinity(0, "Project", "hash"),
            ColocationGroup.forAssignments(Arrays.asList(
                select(nodes, 0, 1),
                select(nodes, 1, 2),
                select(nodes, 2, 0),
                select(nodes, 0, 1),
                select(nodes, 1, 2)
            )),
            5
        );

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0";

        // Data is partitioned and colocated, can be joined on remote nodes, one exchange is required to transfer to
        // initiator node.
        assertPlan(sql, schema, hasFragmentsCount(2).and(nodeOrAnyChild(isInstanceOf(Exchange.class))));
    }

    /** */
    @Test
    public void testSplitterColocatedReplicatedReplicated() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 0, 1, 2, 3)),
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 0, 1, 2, 3))
        );

        String sql = "SELECT d.id, (d.id + 1) as id2, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) = ?";

        // Data is fully replicated, no exchanges requred, can be executed direcly on initiatpr node.
        assertPlan(sql, schema, hasFragmentsCount(1).and(nodeOrAnyChild(isInstanceOf(Exchange.class)).negate()));
    }

    /** */
    @Test
    public void testSplitterPartiallyColocatedReplicatedAndPartitioned() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 0)),
            IgniteDistributions.affinity(0, "Project", "hash"),
            ColocationGroup.forAssignments(Arrays.asList(
                select(nodes, 1, 2),
                select(nodes, 2, 3),
                select(nodes, 3, 0),
                select(nodes, 0, 1)
            )),
            15
        );

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) = ?";

        // First table is replicated and planned with TrimExchange to colocate data, but set of nodes for partitioned
        // table is differ, so exchange is added after fragments split for colocation. Another exchange is added after
        // fragments split to send data to initiator node.
        assertPlan(sql, schema, hasFragmentsCount(3).and(hasChildThat(isInstanceOf(IgniteTrimExchange.class))));
    }

    /** */
    @Test
    public void testSplitterPartiallyColocated1() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 1, 2, 3)),
            IgniteDistributions.affinity(0, "Project", "hash"),
            ColocationGroup.forAssignments(Arrays.asList(
                select(nodes, 0),
                select(nodes, 1),
                select(nodes, 2)
            )),
            10
        );

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) = ?";

        // First table is replicated and planned with TrimExchange to colocate data, but set of nodes for partitioned
        // table is differ, so exchange is added after fragments split for colocation. Another exchange is added after
        // fragments split to send data to initiator node.
        assertPlan(sql, schema, hasFragmentsCount(3).and(hasChildThat(isInstanceOf(IgniteTrimExchange.class))));
    }

    /** */
    @Test
    public void testSplitterPartiallyColocated2() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 0)),
            IgniteDistributions.affinity(0, "Project", "hash"),
            ColocationGroup.forAssignments(Arrays.asList(
                select(nodes, 1),
                select(nodes, 2),
                select(nodes, 3)
            )),
            15
        );

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) = ?";

        // First table is replicated and planned with TrimExchange to colocate data, but set of nodes for partitioned
        // table is differ, so exchange is added after fragments split for colocation. Another exchange is added after
        // fragments split to send data to initiator node.
        assertPlan(sql, schema, hasFragmentsCount(3).and(hasChildThat(isInstanceOf(IgniteTrimExchange.class))));
    }

    /** */
    @Test
    public void testSplitterNonColocatedReplicatedReplicated1() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 2)),
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 0, 1))
        );

        String sql = "SELECT p.id0, d.id " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) = ?";

        // Originally planned without exchanges. But there is no data for table 1 on initiator node, so one exchange
        // is added after fragments split.
        assertPlan(sql, schema, hasFragmentsCount(2).and(nodeOrAnyChild(isInstanceOf(Exchange.class)).negate()));
    }

    /** */
    @Test
    public void testSplitterNonColocatedReplicatedReplicated2() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 2)),
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 1))
        );

        String sql = "SELECT p.id0, d.id " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) = ?";

        // Originally planned without exchanges. But data can't be joined on any single node, so two exchanges is
        // added after fragments split.
        assertPlan(sql, schema, hasFragmentsCount(3).and(nodeOrAnyChild(isInstanceOf(Exchange.class)).negate()));
    }

    /** */
    @Test
    public void testSplitterPartiallyColocatedReplicatedReplicated() throws Exception {
        IgniteSchema schema = createSchema(
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 2, 3)),
            IgniteDistributions.broadcast(),
            ColocationGroup.forNodes(select(nodes, 1, 3))
        );

        String sql = "SELECT p.id0, d.id " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) = ?";

        // Originally planned without exchanges. There is no data on initiator node, but data can be joined on one
        // remote node, so one exchange is added after fragments split.
        assertPlan(sql, schema, hasFragmentsCount(2).and(nodeOrAnyChild(isInstanceOf(Exchange.class)).negate()));
    }

    /**
     * Predicate builder for "Fragments count" condition.
     */
    protected <T extends IgniteRel> Predicate<T> hasFragmentsCount(int cnt) {
        return node -> {
            MultiStepPlan plan = new MultiStepQueryPlan(null, null, new QueryTemplate(new Splitter().go(node)),
                null, null);

            ExecutionPlan execPlan = plan.init(this::intermediateMapping, null,
                Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

            if (execPlan.fragments().size() != cnt) {
                lastErrorMsg = "Unexpected fragments count [expected=" + cnt + ", actual=" + execPlan.fragments().size() + ']';

                return false;
            }

            return true;
        };
    }
}
