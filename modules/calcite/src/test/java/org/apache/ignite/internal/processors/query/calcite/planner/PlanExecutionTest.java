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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.query.calcite.QueryRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.LogicalRelImplementor;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.RootNode;
import org.apache.ignite.internal.processors.query.calcite.exec.task.StripedQueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpIoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExecutionPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryTemplate;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 *
 */
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class PlanExecutionTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalPlan() throws Exception {
        executors = new ArrayList<>();

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup grp,
                ImmutableBitSet requiredColumns
            ) {
                return Arrays.asList(
                    row(execCtx, requiredColumns, 0, "Igor", 0),
                    row(execCtx, requiredColumns, 1, "Roman", 0)
                );
            }

            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 1));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable project = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("VER", f.createJavaType(Integer.class))
                .build()) {
            @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup grp,
                ImmutableBitSet requiredColumns
            ) {
                return Arrays.asList(
                    row(execCtx, requiredColumns, 0, "Calcite", 1),
                    row(execCtx, requiredColumns, 1, "Ignite", 1)
                );
            }

            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 1));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        String sql = "SELECT d.id, d.name, d.projectId, p.name0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.name as name0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        List<Object[]> res = executeQuery(publicSchema, sql, -10);

        assertFalse(res.isEmpty());

        Assert.assertArrayEquals(new Object[]{0, "Igor", 0, "Calcite", 1}, res.get(0));
        Assert.assertArrayEquals(new Object[]{1, "Roman", 0, "Calcite", 1}, res.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalPlan2() throws Exception {
        executors = new ArrayList<>();

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        ThreadLocal<List<?>> checkRes = new ThreadLocal<>();

        TestTable testTbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID0", f.createJavaType(Integer.class))
                .add("ID1", f.createJavaType(Integer.class))
                .build()) {
            @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup grp,
                ImmutableBitSet requiredColumns
            ) {
                List<Row> res = new ArrayList<>();
                List<Row> checkRes0 = new ArrayList<>();

                for (int i = 0; i < 10; ++i) {
                    int col = ThreadLocalRandom.current().nextInt(1_000);

                    res.add(row(execCtx, requiredColumns, col, col));
                    checkRes0.add(row(execCtx, null, col + col));
                }

                checkRes.set(checkRes0);

                return res;
            }

            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 1));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST_TABLE", testTbl);

        String sql = "SELECT (ID0 + ID1) AS RES FROM PUBLIC.TEST_TABLE";

        List<Object[]> res = executeQuery(publicSchema, sql);

        assertFalse(res.isEmpty());

        int pos = 0;

        for (Object obj : checkRes.get())
            Assert.assertArrayEquals((Object[])obj, res.get(pos++));
    }

    /** */
    private List<Object[]> executeQuery(IgniteSchema publicSchema, String sql, Object... parameters) throws Exception {
        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        BaseQueryContext qctx = BaseQueryContext.builder()
            .logger(log)
            .defaultSchema(schema)
            .build();

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(qctx)
            .query(sql)
            .parameters(parameters)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        ExecutionPlan plan = splitPlan(phys);

        List<Fragment> fragments = plan.fragments();
        assertEquals(2, fragments.size());

        UUID qryId = UUID.randomUUID();

        TestIoManager mgr = new TestIoManager();
        Node<Object[]> exec;

        //// Local part

        Fragment fragment = fragments.get(0);
        assert fragment.rootFragment();

        exec = implementFragment(qctx, ctx, mgr, plan, fragment, qryId, F.first(nodes));

        RootNode<Object[]> consumer = new RootNode<>(exec.context(), exec.rowType());
        consumer.register(exec);

        //// Remote part

        fragment = fragments.get(1);

        assert !fragment.rootFragment();

        exec = implementFragment(qctx, ctx, mgr, plan, fragment, qryId, nodes.get(1));

        //// Start execution

        assert exec instanceof Outbox;

        Outbox<Object[]> outbox = (Outbox<Object[]>)exec;

        exec.context().execute(outbox::init, outbox::onError);

        List<Object[]> res = new ArrayList<>();

        while (consumer.hasNext())
            res.add(consumer.next());

        return res;
    }

    /** */
    private ExecutionPlan splitPlan(IgniteRel phys) {
        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(null, null, new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        return plan.init(this::intermediateMapping, null, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));
    }

    /**
     * Transforms fragment to the execution node.
     */
    private Node<Object[]> implementFragment(
        BaseQueryContext qctx,
        PlanningContext ctx,
        TestIoManager mgr,
        ExecutionPlan plan,
        Fragment fragment,
        UUID qryId,
        UUID nodeId
    ) throws IgniteCheckedException {
        GridTestKernalContext kernal = newContext();
        kernal.add(new NoOpIgniteSecurityProcessor(kernal));
        kernal.add(new GridCacheProcessor(kernal));

        StripedQueryTaskExecutor taskExecutor = new StripedQueryTaskExecutor(kernal);
        taskExecutor.stripedThreadPoolExecutor(new IgniteStripedThreadPoolExecutor(
            kernal.config().getQueryThreadPoolSize(),
            kernal.igniteInstanceName(),
            "calciteQry",
            (t, ex) -> {
                log().error(ex.getMessage(), ex);
                lastE = ex;
            },
            true,
            DFLT_THREAD_KEEP_ALIVE_TIME
        ));
        executors.add(taskExecutor);

        MessageServiceImpl msgSvc = new TestMessageServiceImpl(kernal, mgr);

        msgSvc.localNodeId(nodeId);
        msgSvc.taskExecutor(taskExecutor);
        mgr.register(msgSvc);

        MailboxRegistryImpl mailboxRegistry = new MailboxRegistryImpl(kernal);

        ExchangeServiceImpl exchangeSvc = new ExchangeServiceImpl(kernal);
        exchangeSvc.taskExecutor(taskExecutor);
        exchangeSvc.messageService(msgSvc);
        exchangeSvc.mailboxRegistry(mailboxRegistry);
        exchangeSvc.queryRegistry(new QueryRegistryImpl(kernal));
        exchangeSvc.init();

        ExecutionContext<Object[]> ectx = new ExecutionContext<>(
            qctx,
            taskExecutor,
            null,
            qryId,
            nodeId,
            F.first(nodes),
            AffinityTopologyVersion.NONE,
            new FragmentDescription(
                fragment.fragmentId(),
                fragment.mapping(),
                plan.target(fragment),
                plan.remotes(fragment)),
            ArrayRowHandler.INSTANCE,
            NoOpMemoryTracker.INSTANCE,
            NoOpIoTracker.INSTANCE,
            0,
            Commons.parametersMap(ctx.parameters()),
            null);

        return new LogicalRelImplementor<>(ectx, c -> r -> 0, mailboxRegistry, exchangeSvc,
            new TestFailureProcessor(kernal)).go(fragment.root());
    }
}
