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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.fromFullName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.histogramBucketNames;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assert.assertArrayEquals;

/** */
public class MetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private MetricRegistry mreg;

    /** */
    @Before
    public void setUp() throws Exception {
        mreg = new MetricRegistry("group", name -> null, name -> null, null);
    }

    /** */
    @Test
    public void testLongCounter() throws Exception {
        AtomicLongMetric l = mreg.longMetric("ltest", "test");

        run(l::increment, 100);

        assertEquals(100 * 100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testLongAdderCounter() throws Exception {
        LongAdderMetric l = mreg.longAdderMetric("latest", "test");

        run(l::increment, 100);

        assertEquals(100 * 100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testDoubleCounter() throws Exception {
        DoubleMetricImpl l = mreg.doubleMetric("dtest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100 * 100f, l.value(), .000001);

        l.reset();

        assertEquals(0, l.value(), .000001);
    }

    /** */
    @Test
    public void testIntCounter() throws Exception {
        IntMetricImpl l = mreg.intMetric("itest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100 * 100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testRegister() throws Exception {
        AtomicLongMetric l = new AtomicLongMetric("rtest", "test");

        mreg.register(l);

        assertEquals(l, mreg.findMetric("rtest"));

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testBooleanMetric() throws Exception {
        final boolean[] v = new boolean[1];

        mreg.register("bmtest", () -> v[0], "test");

        BooleanMetric m = mreg.findMetric("bmtest");

        assertEquals(v[0], m.value());

        v[0] = true;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testDoubleMetric() throws Exception {
        final double[] v = new double[] {42};

        mreg.register("dmtest", () -> v[0], "test");

        DoubleMetric m = mreg.findMetric("dmtest");

        assertEquals(v[0], m.value(), 0);

        v[0] = 1;

        assertEquals(v[0], m.value(), 0);
    }

    /** */
    @Test
    public void testIntMetric() throws Exception {
        final int[] v = new int[] {42};

        mreg.register("imtest", () -> v[0], "test");

        IntMetric m = mreg.findMetric("imtest");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testLongMetric() throws Exception {
        final long[] v = new long[] {42};

        mreg.register("lmtest", () -> v[0], "test");

        LongMetric m = mreg.findMetric("lmtest");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testObjectMetric() throws Exception {
        final String[] v = new String[] {"42"};

        mreg.register("omtest", () -> v[0], String.class, "test");

        ObjectMetric<String> m = mreg.findMetric("omtest");

        assertEquals(v[0], m.value());

        v[0] = "1";

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testBooleanGauges() throws Exception {
        BooleanMetricImpl bg = mreg.booleanMetric("bg", "test");

        bg.value(true);

        assertTrue(bg.value());

        bg.reset();

        assertFalse(bg.value());
    }

    /** */
    @Test
    public void testHistogram() throws Exception {
        HistogramMetricImpl h = mreg.histogram("hmtest", new long[] {10, 100, 500}, "test");

        List<IgniteInternalFuture> futs = new ArrayList<>();

        int cnt = 10;

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt; i++)
                h.value(9);
        }));

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt * 2; i++)
                h.value(99);
        }));

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt * 3; i++)
                h.value(500);
        }));

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt * 4; i++)
                h.value(501);
        }));

        for (IgniteInternalFuture fut : futs)
            fut.get();

        long[] res = h.value();

        assertEquals(cnt, res[0]);
        assertEquals(cnt * 2, res[1]);
        assertEquals(cnt * 3, res[2]);
        assertEquals(cnt * 4, res[3]);
    }

    /** */
    @Test
    public void testGetMetrics() throws Exception {
        MetricRegistry mreg = new MetricRegistry("group", name -> null, name -> null, null);

        mreg.longMetric("test1", "");
        mreg.longMetric("test2", "");
        mreg.longMetric("test3", "");
        mreg.longMetric("test4", "");
        mreg.longMetric("test5", "");

        Set<String> names = new HashSet<>(asList("group.test1", "group.test2", "group.test3", "group.test4",
            "group.test5"));

        Set<String> res = StreamSupport.stream(Spliterators.spliteratorUnknownSize(mreg.iterator(), 0), false)
            .map(Metric::name)
            .collect(toSet());

        assertEquals(names, res);
    }

    /** */
    @Test
    public void testRemove() throws Exception {
        MetricRegistry mreg = new MetricRegistry("group", name -> null, name -> null, null);

        AtomicLongMetric cntr = mreg.longMetric("my.name", null);
        AtomicLongMetric cntr2 = mreg.longMetric("my.name.x", null);

        assertNotNull(cntr);
        assertNotNull(cntr2);

        assertNotNull(mreg.findMetric("my.name"));
        assertNotNull(mreg.findMetric("my.name.x"));

        mreg.remove("my.name");

        assertNull(mreg.findMetric("my.name"));
        assertNotNull(mreg.findMetric("my.name.x"));

        cntr = mreg.longMetric("my.name", null);

        assertNotNull(mreg.findMetric("my.name"));
    }

    /** */
    @Test
    public void testHitRateMetric() throws Exception {
        long rateTimeInterval = 500;

        HitRateMetric metric = mreg.hitRateMetric("testHitRate", null, rateTimeInterval, 10);

        assertEquals(0, metric.value());

        long startTs = U.currentTimeMillis();

        GridTestUtils.runMultiThreaded(metric::increment, 10, "test-thread");

        assertTrue(metric.value() > 0 || U.currentTimeMillis() - startTs > rateTimeInterval);

        U.sleep(rateTimeInterval * 2);

        assertEquals(0, metric.value());

        assertEquals(rateTimeInterval, metric.rateTimeInterval());

        metric.reset(rateTimeInterval * 2, 10);

        assertEquals(rateTimeInterval * 2, metric.rateTimeInterval());
    }

    /** */
    @Test
    public void testHistogramNames() throws Exception {
        HistogramMetricImpl h = new HistogramMetricImpl("test", null, new long[]{10, 50, 500});

        String[] names = histogramBucketNames(h);

        assertArrayEquals(new String[] {
            "test_0_10",
            "test_10_50",
            "test_50_500",
            "test_500_inf"
        }, names);
    }

    /** */
    @Test
    public void testFromFullName() {
        assertEquals(new T2<>("org.apache", "ignite"), fromFullName("org.apache.ignite"));

        assertEquals(new T2<>("org", "apache"), fromFullName("org.apache"));
    }

    /** */
    @Test
    public void testAddBeforeRemoveCompletes() throws Exception {
        MetricExporterSpi checkSpi = new NoopMetricExporterSpi() {
            private ReadOnlyMetricManager registry;

            private Set<String> names = new HashSet<>();

            @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
                registry.addMetricRegistryCreationListener(mreg -> {
                    assertFalse(mreg.name() + " should be unique", names.contains(mreg.name()));

                    names.add(mreg.name());
                });

                registry.addMetricRegistryRemoveListener(mreg -> names.remove(mreg.name()));
            }

            @Override public void setMetricRegistry(ReadOnlyMetricManager registry) {
                this.registry = registry;
            }
        };

        CountDownLatch rmvStarted = new CountDownLatch(1);
        CountDownLatch rmvCompleted = new CountDownLatch(1);

        MetricExporterSpi blockingSpi = new NoopMetricExporterSpi() {
            private ReadOnlyMetricManager registry;

            @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
                registry.addMetricRegistryRemoveListener(mreg -> {
                    rmvStarted.countDown();
                    try {
                        rmvCompleted.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            @Override public void setMetricRegistry(ReadOnlyMetricManager registry) {
                this.registry = registry;
            }
        };

        IgniteConfiguration cfg = new IgniteConfiguration().setMetricExporterSpi(blockingSpi, checkSpi);

        GridTestKernalContext ctx = new GridTestKernalContext(log(), cfg);

        ctx.start();

        // Add metric registry.
        ctx.metric().registry("test");

        // Removes it async, blockingSpi will block remove procedure.
        IgniteInternalFuture rmvFut = runAsync(() -> ctx.metric().remove("test"));

        rmvStarted.await();

        CountDownLatch addStarted = new CountDownLatch(1);

        IgniteInternalFuture addFut = runAsync(() -> {
            addStarted.countDown();

            ctx.metric().registry("test");
        });

        // Waiting for creation to start.
        addStarted.await();

        Thread.sleep(100);

        // Complete removal.
        rmvCompleted.countDown();

        rmvFut.get(getTestTimeout());

        addFut.get(getTestTimeout());
    }

    /** */
    private void run(Runnable r, int cnt) throws org.apache.ignite.IgniteCheckedException {
        List<IgniteInternalFuture> futs = new ArrayList<>();

        for (int i = 0; i < cnt; i++) {
            futs.add(runAsync(() -> {
                for (int j = 0; j < cnt; j++)
                    r.run();
            }));
        }

        for (IgniteInternalFuture fut : futs)
            fut.get();
    }
}
