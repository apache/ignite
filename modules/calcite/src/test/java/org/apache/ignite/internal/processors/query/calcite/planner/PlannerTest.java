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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.QueryRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.LogicalRelImplementor;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.RootNode;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryTemplate;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class PlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterColocatedPartitionedPartitioned() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Developer", "hash");
            }
        };

        TestTable project = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("VER", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .logger(log)
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build())
                .build())
            .query(sql)
            .parameters(2)
            .build();

        assertNotNull(ctx);

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

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
                ColocationGroup group,
                Predicate<Row> filter,
                Function<Row, Row> transformer,
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
                ColocationGroup group,
                Predicate<Row> filter,
                Function<Row, Row> transformer,
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

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.name0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.name as name0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        BaseQueryContext qctx = BaseQueryContext.builder()
            .logger(log)
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .build())
            .build();

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(qctx)
            .query(sql)
            .parameters(-10)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);
        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        List<Fragment> fragments = plan.fragments();
        assertEquals(2, fragments.size());

        UUID qryId = UUID.randomUUID();

        TestIoManager mgr = new TestIoManager();
        GridTestKernalContext kernal;
        QueryTaskExecutorImpl taskExecutor;
        MessageServiceImpl msgSvc;
        MailboxRegistryImpl mailboxRegistry;
        ExchangeServiceImpl exchangeSvc;
        ExecutionContext<Object[]> ectx;
        Node<Object[]> exec;

        //// Local part

        Fragment fragment = fragments.get(0);
        assert fragment.rootFragment();

        kernal = newContext();

        taskExecutor = new QueryTaskExecutorImpl(kernal);
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

        msgSvc = new TestMessageServiceImpl(kernal, mgr);

        msgSvc.localNodeId(nodes.get(0));
        msgSvc.taskExecutor(taskExecutor);
        mgr.register(msgSvc);

        mailboxRegistry = new MailboxRegistryImpl(kernal);

        exchangeSvc = new ExchangeServiceImpl(kernal);
        exchangeSvc.taskExecutor(taskExecutor);
        exchangeSvc.messageService(msgSvc);
        exchangeSvc.mailboxRegistry(mailboxRegistry);
        exchangeSvc.queryRegistry(new QueryRegistryImpl(kernal));
        exchangeSvc.init();

        ectx = new ExecutionContext<>(
            qctx,
            taskExecutor,
            qryId,
            F.first(nodes),
            F.first(nodes),
            AffinityTopologyVersion.NONE,
            new FragmentDescription(
                fragment.fragmentId(),
                fragment.mapping(),
                plan.target(fragment),
                plan.remotes(fragment)),
            ArrayRowHandler.INSTANCE,
            Commons.parametersMap(ctx.parameters())
        );

        exec = new LogicalRelImplementor<>(ectx, c1 -> r1 -> 0, mailboxRegistry, exchangeSvc,
            new TestFailureProcessor(kernal)).go(fragment.root());

        RootNode<Object[]> consumer = new RootNode<>(ectx, exec.rowType());
        consumer.register(exec);

        //// Remote part

        fragment = fragments.get(1);

        assert !fragment.rootFragment();

        kernal = newContext();

        taskExecutor = new QueryTaskExecutorImpl(kernal);
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

        msgSvc = new TestMessageServiceImpl(kernal, mgr);
        msgSvc.localNodeId(nodes.get(1));
        msgSvc.taskExecutor(taskExecutor);
        mgr.register(msgSvc);

        mailboxRegistry = new MailboxRegistryImpl(kernal);

        exchangeSvc = new ExchangeServiceImpl(kernal);
        exchangeSvc.taskExecutor(taskExecutor);
        exchangeSvc.messageService(msgSvc);
        exchangeSvc.mailboxRegistry(mailboxRegistry);
        exchangeSvc.queryRegistry(new QueryRegistryImpl(kernal));
        exchangeSvc.init();

        ectx = new ExecutionContext<>(
            qctx,
            taskExecutor,
            qryId,
            nodes.get(1),
            F.first(nodes),
            AffinityTopologyVersion.NONE,
            new FragmentDescription(
                fragment.fragmentId(),
                fragment.mapping(),
                plan.target(fragment),
                plan.remotes(fragment)),
            ArrayRowHandler.INSTANCE,
            Commons.parametersMap(ctx.parameters()));

        exec = new LogicalRelImplementor<>(ectx, c -> r -> 0, mailboxRegistry, exchangeSvc,
            new TestFailureProcessor(kernal)).go(fragment.root());

        //// Start execution

        assert exec instanceof Outbox;

        Outbox<Object[]> outbox = (Outbox<Object[]>)exec;

        exec.context().execute(outbox::init, outbox::onError);

        ArrayList<Object[]> res = new ArrayList<>();

        while (consumer.hasNext())
            res.add(consumer.next());

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
                ColocationGroup group,
                Predicate<Row> filter,
                Function<Row, Row> rowTransformer,
                ImmutableBitSet requiredColumns
            ) {
                List<Row> checkRes0 = new ArrayList<>();

                for (int i = 0; i < 10; ++i) {
                    int col = ThreadLocalRandom.current().nextInt(1_000);

                    Row r = row(execCtx, requiredColumns, col, col);

                    if (rowTransformer != null)
                        r = rowTransformer.apply(r);

                    checkRes0.add(r);
                }

                checkRes.set(checkRes0);

                return checkRes0;
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

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT (ID0 + ID1) AS RES FROM PUBLIC.TEST_TABLE";

        BaseQueryContext qctx = BaseQueryContext.builder()
            .logger(log)
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .build())
            .build();

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(qctx)
            .query(sql)
            .parameters(-10)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        List<Fragment> fragments = plan.fragments();
        assertEquals(2, fragments.size());

        UUID qryId = UUID.randomUUID();

        TestIoManager mgr = new TestIoManager();
        GridTestKernalContext kernal;
        QueryTaskExecutorImpl taskExecutor;
        MessageServiceImpl msgSvc;
        MailboxRegistryImpl mailboxRegistry;
        ExchangeServiceImpl exchangeSvc;
        ExecutionContext<Object[]> ectx;
        Node<Object[]> exec;

        //// Local part

        Fragment fragment = fragments.get(0);
        assert fragment.rootFragment();

        kernal = newContext();

        taskExecutor = new QueryTaskExecutorImpl(kernal);
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

        msgSvc = new TestMessageServiceImpl(kernal, mgr);

        msgSvc.localNodeId(nodes.get(0));
        msgSvc.taskExecutor(taskExecutor);
        mgr.register(msgSvc);

        mailboxRegistry = new MailboxRegistryImpl(kernal);

        exchangeSvc = new ExchangeServiceImpl(kernal);
        exchangeSvc.taskExecutor(taskExecutor);
        exchangeSvc.messageService(msgSvc);
        exchangeSvc.mailboxRegistry(mailboxRegistry);
        exchangeSvc.queryRegistry(new QueryRegistryImpl(kernal));
        exchangeSvc.init();

        ectx = new ExecutionContext<>(
            qctx,
            taskExecutor,
            qryId,
            F.first(nodes),
            F.first(nodes),
            AffinityTopologyVersion.NONE,
            new FragmentDescription(
                fragment.fragmentId(),
                fragment.mapping(),
                plan.target(fragment),
                plan.remotes(fragment)),
            ArrayRowHandler.INSTANCE,
            Commons.parametersMap(ctx.parameters()));

        exec = new LogicalRelImplementor<>(ectx, c1 -> r1 -> 0, mailboxRegistry, exchangeSvc,
            new TestFailureProcessor(kernal)).go(fragment.root());

        RootNode<Object[]> consumer = new RootNode<>(ectx, exec.rowType());
        consumer.register(exec);

        //// Remote part

        fragment = fragments.get(1);

        assert !fragment.rootFragment();

        kernal = newContext();

        taskExecutor = new QueryTaskExecutorImpl(kernal);
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

        msgSvc = new TestMessageServiceImpl(kernal, mgr);
        msgSvc.localNodeId(nodes.get(1));
        msgSvc.taskExecutor(taskExecutor);
        mgr.register(msgSvc);

        mailboxRegistry = new MailboxRegistryImpl(kernal);

        exchangeSvc = new ExchangeServiceImpl(kernal);
        exchangeSvc.taskExecutor(taskExecutor);
        exchangeSvc.messageService(msgSvc);
        exchangeSvc.mailboxRegistry(mailboxRegistry);
        exchangeSvc.queryRegistry(new QueryRegistryImpl(kernal));
        exchangeSvc.init();

        ectx = new ExecutionContext<>(
            qctx,
            taskExecutor,
            qryId,
            nodes.get(1),
            F.first(nodes),
            AffinityTopologyVersion.NONE,
            new FragmentDescription(
                fragment.fragmentId(),
                fragment.mapping(),
                plan.target(fragment),
                plan.remotes(fragment)),
            ArrayRowHandler.INSTANCE,
            Commons.parametersMap(ctx.parameters()));

        exec = new LogicalRelImplementor<>(ectx, c -> r -> 0, mailboxRegistry, exchangeSvc,
            new TestFailureProcessor(kernal)).go(fragment.root());

        //// Start execution

        assert exec instanceof Outbox;

        Outbox<Object[]> outbox = (Outbox<Object[]>)exec;

        exec.context().execute(outbox::init, outbox::onError);

        ArrayList<Object[]> res = new ArrayList<>();

        while (consumer.hasNext())
            res.add(consumer.next());

        assertFalse(res.isEmpty());

        int pos = 0;

        for (Object obj : checkRes.get())
            Assert.assertArrayEquals((Object[])obj, res.get(pos++));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterColocatedReplicatedReplicated() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 0, 1, 2, 3));
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
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 0, 1, 2, 3));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, (d.id + 1) as id2, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build())
                .logger(log)
                .build())
            .query(sql)
            .parameters(2)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        assertNotNull(plan);

        assertEquals(1, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterPartiallyColocatedReplicatedAndPartitioned() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 0));
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
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 1, 2),
                    select(nodes, 2, 3),
                    select(nodes, 3, 0),
                    select(nodes, 0, 1)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build())
                .logger(log)
                .build())
            .query(sql)
            .parameters(2)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        assertEquals(3, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterPartiallyColocated1() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 1, 2, 3));
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
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 0),
                    select(nodes, 1),
                    select(nodes, 2)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build())
                .logger(log)
                .build())
            .query(sql)
            .parameters(2)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        assertNotNull(plan);

        assertEquals(3, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterPartiallyColocated2() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 0));
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
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 1),
                    select(nodes, 2),
                    select(nodes, 3)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build())
                .logger(log)
                .build())
            .query(sql)
            .parameters(2)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        assertEquals(3, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterNonColocated() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 2));
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
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forNodes(select(nodes, 0, 1));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT p.id0, d.id " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .logger(log)
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build())
                .build())
            .query(sql)
            .parameters(2)
            .build();

        IgniteRel phys = physicalPlan(ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(new Splitter().go(phys)), null, null);

        assertNotNull(plan);

        plan.init(this::intermediateMapping, Commons.mapContext(F.first(nodes), AffinityTopologyVersion.NONE));

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMergeFilters() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable testTbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL", f.createJavaType(String.class))
                .build()) {
            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.single();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", testTbl);

        String sql = "" +
            "SELECT val from (\n" +
            "   SELECT * \n" +
            "   FROM TEST \n" +
            "   WHERE VAL = 10) \n" +
            "WHERE VAL = 10";

        RelNode phys = physicalPlan(sql, publicSchema);

        assertNotNull(phys);

        AtomicInteger filterCnt = new AtomicInteger();

        // Counts filters af the plan.
        phys.childrenAccept(
            new RelVisitor() {
                @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof IgniteFilter)
                        filterCnt.incrementAndGet();

                    super.visit(node, ordinal, parent);
                }
            }
        );

        // Checks that two filter merged into one filter.
        // Expected plan:
        // IgniteProject(VAL=[$1])
        //  IgniteProject(ID=[$0], VAL=[$1])
        //    IgniteFilter(condition=[=(CAST($1):INTEGER, 10)])
        //      IgniteTableScan(table=[[PUBLIC, TEST]])
        assertEquals(0, filterCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinPushExpressionRule() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable emp = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("DEPTNO", f.createJavaType(Integer.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable dept = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("DEPTNO", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("EMP", emp);
        publicSchema.addTable("DEPT", dept);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "select d.deptno, e.deptno " +
            "from dept d, emp e " +
            "where d.deptno + e.deptno = 2";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .costFactory(new IgniteCostFactory(1, 100, 1, 1))
                    .build())
                .logger(log)
                .build()
            )
            .query(sql)
            .build();

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);

            String qry = ctx.query();

            assertNotNull(qry);

            // Parse
            SqlNode sqlNode = planner.parse(qry);

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            assertNotNull(rel);
            assertEquals("" +
                    "LogicalProject(DEPTNO=[$0], DEPTNO0=[$4])\n" +
                    "  LogicalFilter(condition=[=(CAST(+($0, $4)):INTEGER, 2)])\n" +
                    "    LogicalJoin(condition=[true], joinType=[inner])\n" +
                    "      IgniteLogicalTableScan(table=[[PUBLIC, DEPT]])\n" +
                    "      IgniteLogicalTableScan(table=[[PUBLIC, EMP]])\n",
                RelOptUtil.toString(rel));

            // Transformation chain
            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(CorrelationTrait.UNCORRELATED)
                .simplify();

            IgniteRel phys = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            assertNotNull(phys);
            assertEquals(
                "Invalid plan:\n" + RelOptUtil.toString(phys),
                "IgniteProject(DEPTNO=[$3], DEPTNO0=[$2])\n" +
                    "  IgniteCorrelatedNestedLoopJoin(condition=[=(CAST(+($3, $2)):INTEGER, 2)], joinType=[inner], " +
                    "variablesSet=[[$cor2]], correlationVariables=[[$cor2]])\n" +
                    "    IgniteTableScan(table=[[PUBLIC, EMP]])\n" +
                    "    IgniteTableScan(table=[[PUBLIC, DEPT]], filters=[=(CAST(+($t0, $cor2.DEPTNO)):INTEGER, 2)])\n",
                RelOptUtil.toString(phys));

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    /** */
    @Test
    public void testMergeJoinIsNotAppliedForNonEquiJoin() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable emp = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("DEPTNO", f.createJavaType(Integer.class))
                .build(), 1000) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        emp.addIndex("emp_idx", 1, 2);

        TestTable dept = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("DEPTNO", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .build(), 100) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        dept.addIndex("dep_idx", 1, 0);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("EMP", emp);
        publicSchema.addTable("DEPT", dept);

        String sql = "select d.deptno, d.name, e.id, e.name from dept d join emp e " +
            "on d.deptno = e.deptno and e.name >= d.name order by e.name, d.deptno";

        RelNode phys = physicalPlan(sql, publicSchema, "CorrelatedNestedLoopJoin");

        assertNotNull(phys);
        assertEquals("" +
                "IgniteSort(sort0=[$3], sort1=[$0], dir0=[ASC-nulls-first], dir1=[ASC-nulls-first])\n" +
                "  IgniteProject(DEPTNO=[$3], NAME=[$4], ID=[$0], NAME0=[$1])\n" +
                "    IgniteNestedLoopJoin(condition=[AND(=($3, $2), >=($1, $4))], joinType=[inner])\n" +
                "      IgniteTableScan(table=[[PUBLIC, EMP]])\n" +
                "      IgniteTableScan(table=[[PUBLIC, DEPT]])\n",
            RelOptUtil.toString(phys));
    }

    /** */
    @Test
    public void testNotStandardFunctions() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
            "TEST",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .add("VAL", f.createJavaType(String.class))
                    .build()) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.affinity(0, "TEST", "hash");
                }
            }
        );

        String queries[] = {
            "select REVERSE(val) from TEST", // MYSQL
            "select DECODE(id, 0, val, '') from TEST" // ORACLE
        };

        for (String sql : queries) {
            IgniteRel phys = physicalPlan(
                sql,
                publicSchema
            );

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    /** */
    @Test
    public void testMinusDateSerialization() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        IgniteRel phys = physicalPlan("SELECT (DATE '2021-03-01' - DATE '2021-01-01') MONTHS", publicSchema);

        checkSplitAndSerialization(phys, publicSchema);
    }
}
