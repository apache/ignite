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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.metric.IgniteMetrics;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CUSTOM_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
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

        customRegs.forEach(r -> grid(0).metrics().remove(r));

        grid(0).services().cancelAll();

        super.afterTest();
    }

    /**
     * Tests custom metric with service.
     */
    @Test
    public void testWithService() {
        assertFalse(grid(0).metrics().getOrCreate(TestCustomMetricsService.regName("svc")).iterator().hasNext());

        grid(0).metrics().remove("svc");

        grid(0).services().deployNodeSingleton("svc", new TestCustomMetricsService());

        TestService svc = grid(0).services().serviceProxy("svc", TestService.class, true);

        MetricRegistry svcReg = grid(0).metrics().getOrCreate(TestCustomMetricsService.regName("svc"));

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

        metrics.getOrCreate("admin").register("intMetric", metric::get, null);

        IntMetric read = metrics.getOrCreate("admin").findMetric("intMetric");

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
        GridMetricManager sysMetrics = grid(0).context().metric();

        assertNotNull(sysMetrics.registry(GridMetricManager.SYS_METRICS).findMetric(GridMetricManager.GC_CPU_LOAD));

        String customShortName = MetricUtils.metricName(GridMetricManager.GC_CPU_LOAD + '2');
        String unexpectedSysFullName = MetricUtils.metricName(GridMetricManager.SYS_METRICS, customShortName);

        AtomicInteger val = new AtomicInteger();

        customMetrics.getOrCreate(GridMetricManager.SYS_METRICS).register(customShortName, val::get, null);

        assertNull(sysMetrics.registry(GridMetricManager.SYS_METRICS).findMetric(unexpectedSysFullName));

        String fullCustomName = MetricUtils.metricName(CUSTOM_METRICS, GridMetricManager.SYS_METRICS, customShortName);

        assertEquals(fullCustomName, customMetrics.getOrCreate(GridMetricManager.SYS_METRICS).findMetric(customShortName).name());

        for (ReadOnlyMetricRegistry r : customMetrics)
            assertTrue(r.name().startsWith(CUSTOM_METRICS));

        assertNotNull(sysMetrics.find(MetricUtils.metricName(CUSTOM_METRICS, GridMetricManager.SYS_METRICS, customShortName),
            IntMetric.class));
    }

    /** Tests concurrent metric registration. */
    @Test
    public void testIncompatibleMetricTypes() {
        IgniteMetrics metrics = grid(0).metrics();

        AtomicInteger intGauge = new AtomicInteger();
        LongAdder longGauge = new LongAdder();

        metrics.getOrCreate("test").register("intMetric", intGauge::get, null);

        assertNotNull(metrics.getOrCreate("test").findMetric("intMetric"));

        assertThrows(
            null,
            () -> metrics.getOrCreate("test").register("intMetric", longGauge::sum, null),
            IgniteException.class,
            "Other metric with name 'intMetric' is already registered"
        );
    }

    /** Tests custom metric names. */
    @Test
    public void testMetricName() {
        IgniteMetrics metrics = grid(0).metrics();

        AtomicInteger val = new AtomicInteger();

        String errTxt = "Spaces, nulls, empty name or name parts are not allowed";

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate("").register("intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate(null).register("intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate("myreg").register(null, val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate(CUSTOM_METRICS + ' ').register("intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate(" \t myreg ").register("intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate(" \t ").register("intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate("a.b").register(" \t  c . \t d \t .  \t intMetric", val::get, null);
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertNotNull(metrics.getOrCreate(CUSTOM_METRICS + ".a.b"));

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate("myreg").register("intValues..intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate("myreg.").register("intValues.intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate(".myreg.").register("intValues.intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        assertThrows(
            null,
            () -> {
                metrics.getOrCreate("myreg..reg2").register("intValues.intMetric1", val::get, "intMetric1Desc");
                return null;
            },
            IllegalArgumentException.class,
            errTxt
        );

        metrics.getOrCreate("numbers").register("intMetric1", val::get, "intMetric1Desc");

        assertEquals(metricName(CUSTOM_METRICS, "numbers", "intMetric1"),
            metrics.getOrCreate("numbers").findMetric("intMetric1").name());
        assertEquals("intMetric1Desc", metrics.getOrCreate("numbers").findMetric("intMetric1").description());

        metrics.getOrCreate("abc").register("intMetric1", val::get, null);
        metrics.getOrCreate(CUSTOM_METRICS + ".abc").register("intMetric1", val::get, null);
        metrics.getOrCreate("abc").register("intMetric2", val::get, null);
        metrics.getOrCreate(CUSTOM_METRICS + ".abc").register("intMetric2", val::get, null);

        assertEquals(CUSTOM_METRICS + ".abc.intMetric2",
            metrics.getOrCreate("abc").findMetric("intMetric2").name());
        assertEquals(CUSTOM_METRICS + ".abc.intMetric1",
            metrics.getOrCreate("abc").findMetric("intMetric1").name());

        assertFalse(metrics.getOrCreate(CUSTOM_METRICS + ".custom.abc").iterator().hasNext());

        metrics.getOrCreate("custom").register("intMetric10", val::get, null);
        assertEquals(CUSTOM_METRICS + ".custom.intMetric10",
            metrics.getOrCreate("custom").findMetric("intMetric10").name());

        metrics.getOrCreate("abc").register("cde.intMetric1", val::get, null);

        assertEquals(CUSTOM_METRICS + ".abc.cde.intMetric1",
            metrics.getOrCreate("abc").findMetric("cde.intMetric1").name());

        metrics.getOrCreate("abc.cde").register("intMetric1", val::get, null);

        assertEquals(CUSTOM_METRICS + ".abc.cde.intMetric1",
            metrics.getOrCreate("abc").findMetric("cde.intMetric1").name());

        assertNotNull(metrics.getOrCreate("a.b"));

        metrics.getOrCreate("CuStOm").register("intMetric300", val::get, null);

        assertEquals(CUSTOM_METRICS + ".CuStOm.intMetric300",
            metrics.getOrCreate("CuStOm").findMetric("intMetric300").name());
    }

    /** Tests null supplier metric. */
    @Test
    public void testNullSupplier() {
        IgniteMetrics metrics = grid(0).metrics();

        metrics.getOrCreate("test").register("intMetric", (IntSupplier)null, "intMetric1Desc");

        assertNotNull(metrics.getOrCreate("test").findMetric("intMetric"));

        IntMetric read = metrics.getOrCreate("test").findMetric("intMetric");

        assertEquals(0, read.value());
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

            ignite.metrics().getOrCreate(regName(ctx.name())).register(COUNTER_METRIC_NAME,
                metricValue::get, "Counter of speceific service invocation.");

            ignite.metrics().getOrCreate(regName(ctx.name())).register(LOAD_THRESHOLD_METRIC_NAME,
                () -> metricValue.get() >= 100, "Load flag.");

            ignite.metrics().getOrCreate(regName(ctx.name())).register(LOAD_REMOTE_SYSTEM_CLASS_ID,
                () -> remoteId.get(), UUID.class, "Remote system class id.");
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            refresh();

            ignite.metrics().getOrCreate(regName(ctx.name())).remove(COUNTER_METRIC_NAME);
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
