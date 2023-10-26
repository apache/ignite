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

package org.apache.ignite.internal.managers;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Inconsistent metrics from JVM reported: grid manager handle test
 *
 * Test modifies static final field, used only for development
 */
public class GridManagerMxBeanIllegalArgumentHandleTest {
    /** Patched {@code GridMetricManager}. */
    private GridMetricManager mgr;

    /** Changes field to always failing mock. */
    @Before
    public void setUp() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setMetricExporterSpi(new NoopMetricExporterSpi());

        IgniteLogger log = Mockito.mock(IgniteLogger.class);

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(cfg);
        when(ctx.log(Mockito.anyString())).thenReturn(log);
        when(ctx.log(Mockito.any(Class.class))).thenReturn(log);

        mgr = new GridMetricManager(ctx);

        MemoryMXBean memoryMXBean = createAlwaysFailingMxBean();
        Field memField = GridMetricManager.class.getDeclaredField("mem");
        Object memMxBeanFieldBase = GridUnsafe.staticFieldBase(memField);
        long memMxBeanFieldOffset = GridUnsafe.staticFieldOffset(memField);

        GridUnsafe.putObjectField(memMxBeanFieldBase, memMxBeanFieldOffset, memoryMXBean);
    }

    /** MX bean which is always failing to respond with metrics */
    @NotNull private MemoryMXBean createAlwaysFailingMxBean() {
        final Answer<MemoryUsage> failingAnswer = new Answer<MemoryUsage>() {
            @Override public MemoryUsage answer(InvocationOnMock invocationOnMock) throws Throwable {
                throw new IllegalArgumentException(
                    "java.lang.IllegalArgumentException: committed = 5274103808 should be < max = 5274095616"
                );
            }
        };
        final MemoryMXBean memoryMXBean = Mockito.mock(MemoryMXBean.class);
        when(memoryMXBean.getHeapMemoryUsage()).thenAnswer(failingAnswer);
        when(memoryMXBean.getNonHeapMemoryUsage()).thenAnswer(failingAnswer);
        return memoryMXBean;
    }

    /** Checks illegal state is not propagated. */
    @Test
    public void testIllegalStateIsCatch() {
        final long nHeapMax = mgr.nonHeapMemoryUsage().getMax();
        assertEquals(0, nHeapMax);

        final long heapMax = mgr.heapMemoryUsage().getMax();
        assertEquals(0, heapMax);
    }
}
