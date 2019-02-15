/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.managers;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
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
    /** Original value of {@link GridDiscoveryManager#mem} to be restored after test */
    private Object mxBeanToRestore;

    /** Mem mx bean field in {@link GridDiscoveryManager#mem}, already set accessible */
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


    /** Reflections {@link GridDiscoveryManager#mem} field which was made accessible and mutable */
    @NotNull private Field createAccessibleMemField() throws NoSuchFieldException, IllegalAccessException {
        final Field memField = GridDiscoveryManager.class.getDeclaredField("mem");
        memField.setAccessible(true);

        final Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(memField, memField.getModifiers() & ~Modifier.FINAL);
        return memField;
    }

    /**
     * Restores static field in {@link GridDiscoveryManager#mem}
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
        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        final IgniteLogger log = Mockito.mock(IgniteLogger.class);

        final GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(cfg);
        when(ctx.log(Mockito.anyString())).thenReturn(log);
        when(ctx.log(Mockito.any(Class.class))).thenReturn(log);

        final GridDiscoveryManager mgr = new GridDiscoveryManager(ctx);
        final long nHeapMax = mgr.metrics().getNonHeapMemoryMaximum();
        if (correctSetupOfTestPerformed)
            assertEquals(0, nHeapMax);

        final long heapMax = mgr.metrics().getHeapMemoryMaximum();
        if (correctSetupOfTestPerformed)
            assertEquals(0, heapMax);
    }
}
