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
import java.lang.reflect.Modifier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
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
    /** Original value of {@link GridMetricManager#mem} to be restored after test */
    private Object mxBeanToRestore;

    /** Mem mx bean field in {@link GridMetricManager#mem}, already set accessible */
    private Field memMxBeanField;

    /** If we succeeded to set final field this flag is true, otherwise test assertions will not be performed */
    private boolean correctSetupOfTestPerformed;

    /** Changes field to always failing mock. */
    @Before
    public void setUp() throws Exception {
        try {
            final MemoryMXBean memoryMXBean = createAlwaysFailingMxBean();
            memMxBeanField = createAccessibleMemField();
            mxBeanToRestore = memMxBeanField.get(null);
            memMxBeanField.set(null, memoryMXBean);

            correctSetupOfTestPerformed = memMxBeanField.get(null) == memoryMXBean;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** MX bean which is always failing to respond with metrics */
    @NotNull private MemoryMXBean createAlwaysFailingMxBean() {
        final Answer<MemoryUsage> failingAnswer = new Answer<MemoryUsage>() {
            @Override public MemoryUsage answer(InvocationOnMock invocationOnMock) throws Throwable {
                throw new IllegalArgumentException("java.lang.IllegalArgumentException: committed = 5274103808 should be < max = 5274095616");
            }
        };
        final MemoryMXBean memoryMXBean = Mockito.mock(MemoryMXBean.class);
        when(memoryMXBean.getHeapMemoryUsage()).thenAnswer(failingAnswer);
        when(memoryMXBean.getNonHeapMemoryUsage()).thenAnswer(failingAnswer);
        return memoryMXBean;
    }


    /** Reflections {@link GridMetricManager#mem} field which was made accessible and mutable */
    @NotNull private Field createAccessibleMemField() throws NoSuchFieldException, IllegalAccessException {
        final Field memField = GridMetricManager.class.getDeclaredField("mem");
        memField.setAccessible(true);

        final Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(memField, memField.getModifiers() & ~Modifier.FINAL);
        return memField;
    }

    /**
     * Restores static field in {@link GridMetricManager#mem}
     *
     * @throws Exception if field set failed
     */
    @After
    public void tearDown() throws Exception {
        if (correctSetupOfTestPerformed)
            memMxBeanField.set(null, mxBeanToRestore);
    }

    /** Creates minimal disco manager mock, checks illegal state is not propagated */
    @Test
    public void testIllegalStateIsCatch() {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setMetricExporterSpi(new NoopMetricExporterSpi());

        final IgniteLogger log = Mockito.mock(IgniteLogger.class);

        final GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(cfg);
        when(ctx.log(Mockito.anyString())).thenReturn(log);
        when(ctx.log(Mockito.any(Class.class))).thenReturn(log);

        final GridMetricManager mgr = new GridMetricManager(ctx);
        final long nHeapMax = mgr.nonHeapMemoryUsage().getMax();
        if (correctSetupOfTestPerformed)
            assertEquals(0, nHeapMax);

        final long heapMax = mgr.heapMemoryUsage().getMax();
        if (correctSetupOfTestPerformed)
            assertEquals(0, heapMax);
    }
}
