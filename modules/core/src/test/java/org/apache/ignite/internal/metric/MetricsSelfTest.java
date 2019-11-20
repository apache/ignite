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
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
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
    /** Null logger. */
    private static final NullLogger NULL_LOG = new NullLogger();

    /** Metric registry builder. */
    private MetricRegistryBuilder bldr;

    /** */
    @Before
    public void setUp() throws Exception {
        bldr = MetricRegistryBuilder.newInstance("group", NULL_LOG);
    }

    /** */
    @Test
    public void testLongCounter() throws Exception {
        AtomicLongMetric l = bldr.longMetric("ltest", "test");

        run(l::increment, 100);

        assertEquals(100 * 100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testLongAdderCounter() throws Exception {
        LongAdderMetric l = bldr.longAdderMetric("latest", "test");

        run(l::increment, 100);

        assertEquals(100 * 100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testDoubleCounter() throws Exception {
        DoubleMetricImpl l = bldr.doubleMetric("dtest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100 * 100f, l.value(), .000001);

        l.reset();

        assertEquals(0, l.value(), .000001);
    }

    /** */
    @Test
    public void testIntCounter() throws Exception {
        IntMetricImpl l = bldr.intMetric("itest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100 * 100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testBooleanMetric() throws Exception {
        final boolean[] v = new boolean[1];

        BooleanMetric m = bldr.register("bmtest", () -> v[0], "test");

        assertEquals(v[0], m.value());

        v[0] = true;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testDoubleMetric() throws Exception {
        final double[] v = new double[] {42};

        DoubleMetric m = bldr.register("dmtest", () -> v[0], "test");

        assertEquals(v[0], m.value(), 0);

        v[0] = 1;

        assertEquals(v[0], m.value(), 0);
    }

    /** */
    @Test
    public void testIntMetric() throws Exception {
        final int[] v = new int[] {42};

        IntMetric m = bldr.register("imtest", () -> v[0], "test");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testLongMetric() throws Exception {
        final long[] v = new long[] {42};

        LongMetric m = bldr.register("lmtest", () -> v[0], "test");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testObjectMetric() throws Exception {
        final String[] v = new String[] {"42"};

        ObjectMetric<String> m = bldr.register("omtest", () -> v[0], String.class, "test");

        assertEquals(v[0], m.value());

        v[0] = "1";

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testBooleanGauges() throws Exception {
        BooleanMetricImpl bg = bldr.booleanMetric("bg", "test");

        bg.value(true);

        assertTrue(bg.value());

        bg.reset();

        assertFalse(bg.value());
    }

    /** */
    @Test
    public void testHistogram() throws Exception {
        HistogramMetricImpl h = bldr.histogram("hmtest", new long[] {10, 100, 500}, "test");

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
        MetricRegistryBuilder bldr = MetricRegistryBuilder.newInstance("group", NULL_LOG);

        bldr.longMetric("test1", "");
        bldr.longMetric("test2", "");
        bldr.longMetric("test3", "");
        bldr.longMetric("test4", "");
        bldr.longMetric("test5", "");

        MetricRegistry mreg = bldr.build();

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
        MetricRegistryBuilder bldr = MetricRegistryBuilder.newInstance("group", NULL_LOG);

        AtomicLongMetric cntr = bldr.longMetric("my.name", null);
        AtomicLongMetric cntr2 = bldr.longMetric("my.name.x", null);

        MetricRegistry mreg = bldr.build();

        assertNotNull(cntr);
        assertNotNull(cntr2);

        assertNotNull(mreg.findMetric("my.name"));
        assertNotNull(mreg.findMetric("my.name.x"));
    }

    /** */
    @Test
    public void testHitRateMetric() throws Exception {
        long rateTimeInterval = 500;

        HitRateMetric metric = bldr.hitRateMetric("testHitRate", null, rateTimeInterval, 10);

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
