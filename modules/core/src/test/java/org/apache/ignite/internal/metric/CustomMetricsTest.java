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

package org.apache.ignite.internal.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.IgniteMetricRegistry;
import org.apache.ignite.metric.IgniteMetrics;
import org.apache.ignite.metric.LongSumMetric;
import org.apache.ignite.metric.LongValueMetric;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CUSTOM_METRICS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class CustomMetricsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        List<String> customRegs = new ArrayList<>();

        grid(0).metrics().forEach(r -> {
            if (r.name().startsWith(CUSTOM_METRICS))
                customRegs.add(r.name());
        });

        customRegs.forEach(r -> grid(0).metrics().removeCustomRegistry(r));

        grid(0).services().cancelAll();;

        super.afterTest();
    }

    /**
     * Tests custom metric with service.
     */
    @Test
    public void testWithService() {
        assertNull(grid(0).metrics().findRegistry(TestCustomMetricsService.regName("svc")));

        grid(0).services().deployNodeSingleton("svc", new TestCustomMetricsService());

        TestService svc = grid(0).services().serviceProxy("svc", TestService.class, true);

        IgniteMetricRegistry svcReg = grid(0).metrics().customRegistry(TestCustomMetricsService.regName("svc"));

        IntMetric intMetric = svcReg.findMetric(TestCustomMetricsService.COUNTER_METRIC_NAME);
        BooleanMetric boolMetric = svcReg.findMetric(TestCustomMetricsService.LOAD_THRESHOLD_METRIC_NAME);
        ObjectMetric<UUID> uuidMetric = svcReg.findMetric(TestCustomMetricsService.LOAD_REMOTE_SYSTEM_CLASS_ID);

        assertEquals(UUID.class, uuidMetric.type());
        assertNull(uuidMetric.value());

        for (int i = 1; i <= 100; ++i)
            svc.invoke(i);

        assertEquals(10, intMetric.value());
        assertFalse(boolMetric.value());

        assertNotNull(uuidMetric.value());

        for (int i = 1; i <= 1000; ++i)
            svc.invoke(i);

        assertTrue(boolMetric.value());

        svc.refresh();

        assertEquals(0, intMetric.value());
        assertFalse(boolMetric.value());
        assertNull(uuidMetric.value());

        for (int i = 1; i <= 1000; ++i)
            svc.invoke(i);

        assertTrue(boolMetric.value());
        assertEquals(100, intMetric.value());
        assertNotNull(uuidMetric.value());

        grid(0).services().cancel("svc");

        assertEquals(0, intMetric.value());
        assertFalse(boolMetric.value());
        assertNull(uuidMetric.value());

        assertNull(svcReg.findMetric(TestCustomMetricsService.COUNTER_METRIC_NAME));
    }

    /** Tests int metric. */
    @Test
    public void testIntMetric() {
        IgniteMetrics metrics = grid(0).metrics();

        AtomicInteger metric = new AtomicInteger();

        metrics.customRegistry("admin").register("intMetric", metric::get, null);

        IntMetric read = metrics.customRegistry("admin").findMetric("intMetric");

        metric.set(1);
        assertEquals(1, read.value());

        metric.set(100);
        assertEquals(100, read.value());

        metric.incrementAndGet();
        assertEquals(101, read.value());

        metric.decrementAndGet();
        assertEquals(100, read.value());

        metric.addAndGet(100);
        assertEquals(200, read.value());

        metric.addAndGet(-100);
        assertEquals(100, read.value());
    }

    /** Tests custom metric names. */
    @Test
    public void testCustomMetricNameNotCollidingWithSystemNames() {
        IgniteMetrics customMetrics = grid(0).metrics();
        GridMetricManager systemMetrics = grid(0).context().metric();

        assertNotNull(systemMetrics.registry(GridMetricManager.SYS_METRICS).findMetric(GridMetricManager.GC_CPU_LOAD));

        String name = MetricUtils.metricName(GridMetricManager.GC_CPU_LOAD + '2');
        String fullName = MetricUtils.metricName(GridMetricManager.SYS_METRICS, name);

        AtomicInteger value = new AtomicInteger();

        customMetrics.customRegistry(GridMetricManager.SYS_METRICS).register(name, value::get, null);

        customMetrics.customRegistry(CUSTOM_METRICS).register(fullName, value::get, null);

        assertNull(systemMetrics.registry(GridMetricManager.SYS_METRICS).findMetric(name));

        String fullCustomName = MetricUtils.metricName(CUSTOM_METRICS, fullName);

        assertEquals(fullCustomName, customMetrics.customRegistry(GridMetricManager.SYS_METRICS).findMetric(name).name());

        assertEquals(fullCustomName, customMetrics.customRegistry(CUSTOM_METRICS).findMetric(fullName).name());

        for (ReadOnlyMetricRegistry r : customMetrics)
            assertTrue(r.name().startsWith(CUSTOM_METRICS));

        assertNotNull(systemMetrics.find(MetricUtils.metricName(CUSTOM_METRICS, GridMetricManager.SYS_METRICS, name),
            IntMetric.class));
    }

    /** */
    @Test
    public void testMetricInComputation() {
        IgniteMetrics metrics = grid(0).metrics();

        String customRegName = MetricUtils.metricName(CUSTOM_METRICS, TestCustomMetricsComputeTask.METRIC_REGISTRY);

        assertNull(metrics.findRegistry(CUSTOM_METRICS));
        assertNull(metrics.findRegistry(customRegName));

        long computeResult = grid(0).compute().execute(new TestCustomMetricsComputeTask(), null);

        assertNotNull(computeResult);
        assertTrue(computeResult > 0);

        assertNull(metrics.findRegistry(CUSTOM_METRICS));

        LongMetric curMetric = metrics.findRegistry(customRegName).findMetric(TestCustomMetricsComputeTask.METRIC_CURRENT);
        LongMetric totalMetric = metrics.findRegistry(customRegName).findMetric(TestCustomMetricsComputeTask.METRIC_TOTAL);
        LongMetric ticksMetric = metrics.findRegistry(customRegName).findMetric(TestCustomMetricsComputeTask.METRIC_TICKS);

        assertEquals(computeResult, totalMetric.value());
        assertTrue(ticksMetric.value() > 0L);
        assertEquals(totalMetric.value(), curMetric.value());

        long prevTicks = ticksMetric.value();

        grid(0).compute().execute(new TestCustomMetricsComputeTask(), null);

        assertTrue(totalMetric.value() > computeResult);
        assertTrue(totalMetric.value() > curMetric.value());
        assertTrue(ticksMetric.value() > prevTicks);
    }

    /** Tests concurrent metric registration. */
    @Test
    public void testIncompatibleMetricTypes() {
        IgniteMetrics metrics = grid(0).metrics();

        AtomicInteger intGauge = new AtomicInteger();
        LongAdder longGauge = new LongAdder();

        assertNotNull(metrics.customRegistry("test").register("intMetric", intGauge::get, null));

        assertThrows(
            null,
            () -> metrics.customRegistry("test").register("intMetric", longGauge::sum, null),
            IgniteException.class,
            "Other metric with name 'intMetric' is already registered"
        );

        assertThrows(
            null,
            () -> metrics.customRegistry("test").intMetric("intMetric", null),
            IgniteException.class,
            "Other metric with name 'intMetric' is already registered"
        );

        assertNotNull(metrics.customRegistry("test").intMetric("intMetric2", null));

        assertThrows(
            null,
            () -> metrics.customRegistry("test").register("intMetric2", intGauge::get, null),
            IgniteException.class,
            "Other metric with name 'intMetric2' is already registered"
        );
    }

    /** Tests custom metric names. */
    @Test
    public void testMetricName() {
        IgniteMetrics metrics = grid(0).metrics();

        AtomicInteger value = new AtomicInteger();

        assertThrows(
            null,
            () -> metrics.customRegistry("").register("intMetric1", value::get, "intMetric1Desc"),
            IgniteException.class,
            "is empty or contains spaces"
        );

        assertThrows(
            null,
            () -> metrics.customRegistry(CUSTOM_METRICS + ' ').register("intMetric1", value::get, "intMetric1Desc"),
            IgniteException.class,
            "is empty or contains spaces"
        );

        assertThrows(
            null,
            () -> metrics.customRegistry(" \t ").register("intMetric1", value::get, "intMetric1Desc"),
            IgniteException.class,
            "is empty or contains spaces"
        );

        assertThrows(
            null,
            () -> metrics.customRegistry(null).register("intMetric1", value::get, "intMetric1Desc"),
            IgniteException.class,
            "is empty or contains spaces"
        );

        metrics.customRegistry(CUSTOM_METRICS).register("intMetric1", value::get, "intMetric1Desc");

        assertEquals(CUSTOM_METRICS + ".intMetric1",
            metrics.findRegistry(CUSTOM_METRICS).findMetric("intMetric1").name());
        assertEquals("intMetric1Desc", metrics.findRegistry(CUSTOM_METRICS).findMetric("intMetric1").description());

        metrics.customRegistry("abc").register("intMetric1", value::get, null);
        metrics.customRegistry(CUSTOM_METRICS + ".abc").register("intMetric1", value::get, null);
        metrics.customRegistry("abc").register("intMetric2", value::get, null);
        metrics.customRegistry(CUSTOM_METRICS + ".abc").register("intMetric2", value::get, null);

        assertEquals(CUSTOM_METRICS + ".abc.intMetric2",
            metrics.findRegistry(CUSTOM_METRICS + ".abc").findMetric("intMetric2").name());
        assertEquals(CUSTOM_METRICS + ".abc.intMetric1",
            metrics.findRegistry(CUSTOM_METRICS + ".abc").findMetric("intMetric1").name());

        assertNull(metrics.findRegistry(CUSTOM_METRICS + ".custom.abc"));

        metrics.customRegistry(CUSTOM_METRICS + ".custom").register("intMetric10", value::get, null);
        assertEquals(CUSTOM_METRICS + ".custom.intMetric10",
            metrics.findRegistry(CUSTOM_METRICS + ".custom").findMetric("intMetric10").name());

        metrics.customRegistry("abc").register("cde.intMetric1", value::get, null);

        assertEquals(CUSTOM_METRICS + ".abc.cde.intMetric1",
            metrics.findRegistry(CUSTOM_METRICS + ".abc").findMetric("cde.intMetric1").name());

        metrics.customRegistry("abc.cde").register("intMetric1", value::get, null);

        assertEquals(CUSTOM_METRICS + ".abc.cde.intMetric1",
            metrics.findRegistry(CUSTOM_METRICS + ".abc").findMetric("cde.intMetric1").name());

        assertNull(metrics.findRegistry(CUSTOM_METRICS + ".a.b"));

        assertThrows(
            null,
            () -> metrics.customRegistry("a.b").register(" \t  c . \t d \t .  \t intMetric", value::get, null),
            IgniteException.class,
            "is empty or contains spaces"
        );

        assertNotNull(metrics.findRegistry(CUSTOM_METRICS + ".a.b"));

        metrics.customRegistry(CUSTOM_METRICS).register("intMetric300", value::get, null);

        assertEquals(CUSTOM_METRICS + ".CuStOm.intMetric300",
            metrics.findRegistry(CUSTOM_METRICS + ".CuStOm").findMetric("intMetric300").name());
    }

    /** Tests null supplier metric. */
    @Test
    public void testNullSupplier() {
        IgniteMetrics metrics = grid(0).metrics();

        assertNotNull(metrics.customRegistry("test").register("intMetric", (IntSupplier)null, "intMetric1Desc"));

        IntMetric read = metrics.customRegistry("test").findMetric("intMetric");

        assertEquals(0, read.value());
    }

    /**
     * Test computation.
     */
    private static final class TestCustomMetricsComputeTask extends ComputeTaskAdapter<Void, Long> {
        /** */
        private static final String METRIC_REGISTRY = "task.test";

        /** */
        private static final String METRIC_CURRENT = "current";

        /** */
        private static final String METRIC_TOTAL = "total";

        /** */
        private static final String METRIC_TICKS = "ticks";

        /**
         * Test compute job.
         */
        private static final class TestComputeJob extends ComputeJobAdapter {
            /** Ignite instance. */
            @IgniteInstanceResource
            private Ignite ignite;

            /** {@inheritDoc} */
            @Override public Long execute() throws IgniteException {
                long val = 0;

                // Some job limit.
                long limit = 300 + ThreadLocalRandom.current().nextLong(700);

                LongValueMetric metricCur = ignite.metrics().customRegistry(METRIC_REGISTRY).longMetric(METRIC_CURRENT, null);
                LongSumMetric metricTotal = ignite.metrics().customRegistry(METRIC_REGISTRY).longAdderMetric(METRIC_TOTAL, null);
                LongSumMetric metricTicks = ignite.metrics().customRegistry(METRIC_REGISTRY).longAdderMetric(METRIC_TICKS, null);

                while (!isCancelled() && val < limit) {
                    // Does some job.
                    try {
                        U.sleep(ThreadLocalRandom.current().nextInt(50));
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        //No op.
                    }

                    long increment = ThreadLocalRandom.current().nextLong(100);

                    val += increment;

                    metricTicks.increment();
                }

                metricCur.value(val);

                metricTotal.add(val);

                return isCancelled() ? 0 : val;
            }
        }

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Void arg) throws IgniteException {
            return subgrid.stream().collect(Collectors.toMap(grid -> new TestComputeJob(), Function.identity()));
        }

        /** {@inheritDoc} */
        @Override public @Nullable Long reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.stream().filter(r -> !r.isCancelled() && r.getException() == null).map(r -> (Long)r.getData())
                .reduce(0L, Long::sum);
        }
    }

    /**
     * Test service impl.
     */
    public static final class TestCustomMetricsService implements TestService {
        /** */
        private static final String COUNTER_METRIC_NAME = "filteredInvocation";

        /** */
        private static final String LOAD_THRESHOLD_METRIC_NAME = "loaded";

        /** */
        private static final String LOAD_REMOTE_SYSTEM_CLASS_ID = "retome.classId";

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @ServiceContextResource
        private ServiceContext ctx;

        /** */
        private AtomicReference<UUID> remoteId;

        /** */
        private final AtomicInteger metricValue = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            remoteId = new AtomicReference<>();

            ignite.metrics().customRegistry(regName(ctx.name())).register(COUNTER_METRIC_NAME,
                metricValue::get, "Counter of speceific service invocation.");

            ignite.metrics().customRegistry(regName(ctx.name())).register(LOAD_THRESHOLD_METRIC_NAME,
                () -> metricValue.get() >= 100, "Load flag.");

            ignite.metrics().customRegistry(regName(ctx.name())).register(LOAD_REMOTE_SYSTEM_CLASS_ID,
                () -> remoteId.get(), UUID.class, "Remote system class id.");
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            refresh();

            ignite.metrics().customRegistry(regName(ctx.name())).remove(COUNTER_METRIC_NAME);
        }

        /** {@inheritDoc} */
        @Override public void refresh() {
            metricValue.set(0);

            remoteId.set(null);
        }

        /** */
        @Override public void invoke(int param) {
            if (ctx.isCancelled())
                return;

            remoteId.compareAndSet(null, UUID.randomUUID());

            // Updates metric sometimes.
            if (!ctx.isCancelled() && param % 10 == 0)
                metricValue.set(param / 10);
        }

        /** */
        private static String regName(String svcName) {
            return "service." + svcName;
        }
    }

    /**
     * Test service.
     */
    public interface TestService extends Service {
        /** */
        void invoke(int param);

        /** */
        void refresh();
    }
}
