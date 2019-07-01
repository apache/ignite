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
import org.apache.ignite.internal.processors.metric.MetricGroup;
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
    private MetricGroup mgrp;

    /** */
    @Before
    public void setUp() throws Exception {
        mgrp = new MetricRegistry().group("group");
    }

    /** */
    @Test
    public void testLongCounter() throws Exception {
        LongMetricImpl l = mgrp.metric("ltest", "test");

        run(l::increment, 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testLongAdderCounter() throws Exception {
        LongAdderMetricImpl l = mgrp.longAdderMetric("latest", "test");

        run(l::increment, 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testDoubleCounter() throws Exception {
        DoubleMetricImpl l = mgrp.doubleMetric("dtest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100*100f, l.value(), .000001);

        l.reset();

        assertEquals(0, l.value(), .000001);
    }

    /** */
    @Test
    public void testIntCounter() throws Exception {
        IntMetricImpl l = mgrp.intMetric("itest", "test");

        run(() -> l.add(1), 100);

        assertEquals(100*100, l.value());

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testRegister() throws Exception {
        LongMetricImpl l = new LongMetricImpl("rtest", "test");

        mgrp.register(l);

        assertEquals(l, mgrp.findMetric("rtest"));

        l.reset();

        assertEquals(0, l.value());
    }

    /** */
    @Test
    public void testBooleanMetric() throws Exception {
        final boolean[] v = new boolean[1];

        mgrp.register("bmtest", () -> v[0], "test");

        BooleanMetric m = (BooleanMetric)mgrp.findMetric("bmtest");

        assertEquals(v[0], m.value());

        v[0] = true;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testDoubleMetric() throws Exception {
        final double[] v = new double[] {42};

        mgrp.register("dmtest", () -> v[0], "test");

        DoubleMetric m = (DoubleMetric)mgrp.findMetric("dmtest");

        assertEquals(v[0], m.value(), 0);

        v[0] = 1;

        assertEquals(v[0], m.value(), 0);
    }

    /** */
    @Test
    public void testIntMetric() throws Exception {
        final int[] v = new int[] {42};

        mgrp.register("imtest", () -> v[0], "test");

        IntMetric m = (IntMetric)mgrp.findMetric("imtest");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testLongMetric() throws Exception {
        final long[] v = new long[] {42};

        mgrp.register("lmtest", () -> v[0], "test");

        LongMetric m = (LongMetric)mgrp.findMetric("lmtest");

        assertEquals(v[0], m.value());

        v[0] = 1;

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testObjectMetric() throws Exception {
        final String[] v = new String[] {"42"};

        mgrp.register("omtest", () -> v[0], String.class, "test");

        ObjectMetric<String> m = (ObjectMetric<String>)mgrp.findMetric("omtest");

        assertEquals(v[0], m.value());

        v[0] = "1";

        assertEquals(v[0], m.value());
    }

    /** */
    @Test
    public void testBooleanGauges() throws Exception {
        BooleanMetricImpl bg = mgrp.booleanMetric("bg", "test");

        bg.value(true);

        assertTrue(bg.value());

        bg.reset();

        assertFalse(bg.value());
    }

    /** */
    @Test
    public void testHistogram() throws Exception {
        HistogramMetric h = mgrp.histogram("hmtest", new long[] {10, 100, 500}, "test");

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
        MetricGroup mgrp = new MetricRegistry().group("group");

        mgrp.metric("test1", "");
        mgrp.metric("test2", "");
        mgrp.metric("test3", "");
        mgrp.metric("test4", "");
        mgrp.metric("test5", "");

        Set<String> names = new HashSet<>(asList("group.test1", "group.test2", "group.test3", "group.test4",
            "group.test5"));

        Set<String> res = StreamSupport.stream(Spliterators.spliteratorUnknownSize(mgrp.iterator(), 0), false)
            .map(Metric::name)
            .collect(toSet());

        assertEquals(names, res);
    }

    /** */
    @Test
    public void testCreationListener() throws Exception {
        MetricRegistry mreg = new MetricRegistry();

        mreg.group("test0");

        Set<String> res = new HashSet<>();

        mreg.addMetricGroupCreationListener(g -> res.add(g.name()));

        mreg.group("test1");
        mreg.group("test2");
        mreg.group("test3");
        mreg.group("test4");
        mreg.group("test5");

        Set<String> names = new HashSet<>(asList("test1", "test2", "test3", "test4", "test5"));

        assertEquals(names, res);
    }

    /** */
    @Test
    public void testRemove() throws Exception {
        MetricGroup mgrp = new MetricRegistry().group("group");

        LongMetricImpl cntr = mgrp.metric("my.name", null);
        LongMetricImpl cntr2 = mgrp.metric("my.name.x", null);

        assertNotNull(cntr);
        assertNotNull(cntr2);

        assertNotNull(mgrp.findMetric("my.name"));
        assertNotNull(mgrp.findMetric("my.name.x"));

        mgrp.remove("my.name");

        assertNull(mgrp.findMetric("my.name"));
        assertNotNull(mgrp.findMetric("my.name.x"));

        cntr = mgrp.metric("my.name", null);

        assertNotNull(mgrp.findMetric("my.name"));
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
