/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/** */
public class MetricsSelfTest {
    /** */
    private MetricRegistry mreg;

    /** */
    @Before
    public void setUp() throws Exception {
        mreg = new MetricRegistry("group", null);
    }

    /** */
    @Test
    public void testLongCounter() throws Exception {
        LongMetricImpl l = mreg.metric("ltest", "test");

        run(l::increment, 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testLongAdderCounter() throws Exception {
        LongAdderMetricImpl l = mreg.longAdderMetric("latest", "test");

        run(l::increment, 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testDoubleCounter() throws Exception {
        DoubleMetricImpl l = mreg.doubleMetric("dtest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100*100f, l.value(), .000001);

        l.reset();

        assertEquals(0, l.value(), .000001);
    }

    /** */
    @Test
    public void testIntCounter() throws Exception {
        IntMetricImpl l = mreg.intMetric("itest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testRegister() throws Exception {
        LongMetricImpl l = new LongMetricImpl("rtest", "test");

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
        BooleanMetricImpl bg = mreg.booleanMetric("bg", "test");

        bg.value(true);

        assertTrue(bg.value());

        bg.reset();

        assertFalse(bg.value());
    }

    /** */
    @Test
    public void testHistogram() throws Exception {
        HistogramMetric h = mreg.histogram("hmtest", new long[] {10, 100, 500}, "test");

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
        MetricRegistry mreg = new MetricRegistry("group", null);

        mreg.metric("test1", "");
        mreg.metric("test2", "");
        mreg.metric("test3", "");
        mreg.metric("test4", "");
        mreg.metric("test5", "");

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
        MetricRegistry mreg = new MetricRegistry("group", null);

        LongMetricImpl cntr = mreg.metric("my.name", null);
        LongMetricImpl cntr2 = mreg.metric("my.name.x", null);

        assertNotNull(cntr);
        assertNotNull(cntr2);

        assertNotNull(mreg.findMetric("my.name"));
        assertNotNull(mreg.findMetric("my.name.x"));

        mreg.remove("my.name");

        assertNull(mreg.findMetric("my.name"));
        assertNotNull(mreg.findMetric("my.name.x"));

        cntr = mreg.metric("my.name", null);

        assertNotNull(mreg.findMetric("my.name"));
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
}
