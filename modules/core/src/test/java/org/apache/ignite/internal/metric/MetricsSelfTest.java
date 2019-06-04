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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metrics.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.counter.DoubleCounter;
import org.apache.ignite.spi.metric.counter.IntCounter;
import org.apache.ignite.spi.metric.counter.LongCounter;
import org.apache.ignite.spi.metric.gauge.BooleanGauge;
import org.apache.ignite.spi.metric.gauge.DoubleGauge;
import org.apache.ignite.spi.metric.gauge.HistogramGauge;
import org.apache.ignite.spi.metric.gauge.IntGauge;
import org.apache.ignite.spi.metric.gauge.LongGauge;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.internal.processors.metrics.MetricNameUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** */
@RunWith(Parameterized.class)
public class MetricsSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Prefix {0}")
    public static Iterable<String[]> data() {
        return asList(
            new String[] {""},
            new String[] {"test.prefix"});
    }

    /** Use index. */
    @Parameterized.Parameter
    public String prefix;

    /** */
    private MetricRegistry mreg;

    /** */
    @Before
    public void setUp() throws Exception {
        mreg = new MetricRegistryImpl();

        if (!F.isEmpty(prefix))
            mreg = mreg.withPrefix(prefix);
    }

    /** */
    @Test
    public void testLongCounter() throws Exception {
        LongCounter l = mreg.counter("ltest", "test");

        run(l::increment, 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testDoubleCounter() throws Exception {
        DoubleCounter l = mreg.doubleCounter("dtest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100*100f, l.value(), .000001);

        l.reset();

        assertEquals(0, l.value(), .000001);
    }

    /** */
    @Test
    public void testIntCounter() throws Exception {
        IntCounter l = mreg.intCounter("itest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testRegister() throws Exception {
        LongCounter l = new LongCounter(testMetricName("rtest"), "test");

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

        BooleanMetric m = (BooleanMetric)mreg.findMetric("bmtest");

        assertEquals(v[0], m.value());

        v[0] = true;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testDoubleMetric() throws Exception {
        final double[] v = new double[] {42};

        mreg.register("dmtest", () -> v[0], "test");

        DoubleMetric m = (DoubleMetric)mreg.findMetric("dmtest");

        assertEquals(v[0], m.value(), 0);

        v[0] = 1;

        assertEquals(v[0], m.value(), 0);
    }

    /** */
    @Test
    public void testIntMetric() throws Exception {
        final int[] v = new int[] {42};

        mreg.register("imtest", () -> v[0], "test");

        IntMetric m = (IntMetric)mreg.findMetric("imtest");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testLongMetric() throws Exception {
        final long[] v = new long[] {42};

        mreg.register("lmtest", () -> v[0], "test");

        LongMetric m = (LongMetric)mreg.findMetric("lmtest");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testObjectMetric() throws Exception {
        final String[] v = new String[] {"42"};

        mreg.register("omtest", () -> v[0], String.class, "test");

        ObjectMetric<String> m = (ObjectMetric<String>)mreg.findMetric("omtest");

        assertEquals(v[0], m.value());

        v[0] = "1";

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testBooleanGauges() throws Exception {
        BooleanGauge bg = mreg.booleanGauge("bg", "test");

        bg.value(true);

        assertTrue(bg.value());

        bg.reset();

        assertFalse(bg.value());
    }

    /** */
    @Test
    public void testDoubleGauges() throws Exception {
        DoubleGauge dg = mreg.doubleGauge("dg", "test");

        dg.value(42);

        assertEquals(dg.value(), 42, .000001);

        dg.reset();

        assertEquals(dg.value(), 0, .000001);
    }

    /** */
    @Test
    public void testIntGauges() throws Exception {
        IntGauge ig = mreg.intGauge("ig", "test");

        ig.value(42);

        assertEquals(42, ig.value());

        ig.reset();

        assertEquals(0, ig.value());
    }

    /** */
    @Test
    public void testLongGauges() throws Exception {
        LongGauge lg = mreg.gauge("lg", "test");

        lg.value(42);

        assertEquals(42, lg.value());

        lg.reset();

        assertEquals(0, lg.value());
    }

    /** */
    @Test
    public void testHistogram() throws Exception {
        HistogramGauge h = mreg.histogram("hmtest", new long[] {10, 100, 500}, "test");

        List<IgniteInternalFuture> futs = new ArrayList<>();

        int cnt = 10;

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt; i++)
                h.value(9);
        }));

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt*2; i++)
                h.value(99);
        }));

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt*3; i++)
                h.value(500);
        }));

        futs.add(runAsync(() -> {
            for (int i = 0; i < cnt*4; i++)
                h.value(501);
        }));

        for (IgniteInternalFuture fut : futs)
            fut.get();

        long[] res = h.value();

        assertEquals(cnt, res[0]);
        assertEquals(cnt*2, res[1]);
        assertEquals(cnt*3, res[2]);
        assertEquals(cnt*4, res[3]);
    }

    /** */
    @Test
    public void testGetMetrics() throws Exception {
        MetricRegistry mreg = new MetricRegistryImpl();

        if (!F.isEmpty(prefix))
            mreg = mreg.withPrefix(prefix);

        mreg.counter("test1", "");
        mreg.counter("test2", "");
        mreg.counter("test3", "");
        mreg.counter("test4", "");
        mreg.counter("test5", "");

        Set<String> names = new HashSet<>(asList(
            testMetricName("test1"),
            testMetricName("test2"),
            testMetricName("test3"),
            testMetricName("test4"),
            testMetricName("test5")));

        Set<String> res = mreg.getMetrics().stream()
            .map(Metric::name)
            .collect(toSet());

        assertEquals(names, res);
    }

    /** */
    @Test
    public void testCreationListener() throws Exception {
        MetricRegistry mreg = new MetricRegistryImpl();

        if (!F.isEmpty(prefix))
            mreg = mreg.withPrefix(prefix);

        mreg.counter("test0", "");

        Set<String> res = new HashSet<>();

        mreg.addMetricCreationListener(m -> res.add(m.name()));

        mreg.counter("test1", null);
        mreg.counter("test2", null);
        mreg.counter("test3", null);
        mreg.counter("test4", null);
        mreg.counter("test5", null);

        Set<String> names = new HashSet<>(asList(
            testMetricName("test1"),
            testMetricName("test2"),
            testMetricName("test3"),
            testMetricName("test4"),
            testMetricName("test5")));

        assertEquals(names, res);
    }

    /** */
    private void run(Runnable r, int cnt) throws org.apache.ignite.IgniteCheckedException {
        List<IgniteInternalFuture> futs = new ArrayList<>();

        for (int i=0; i<cnt; i++) {
            futs.add(runAsync(() -> {
                for (int j = 0; j < cnt; j++)
                    r.run();
            }));
        }

        for (IgniteInternalFuture fut : futs)
            fut.get();
    }

    /** */
    private String testMetricName(String name) {
        if (prefix.isEmpty())
            return name;

        return metricName(prefix, name);
    }
}
