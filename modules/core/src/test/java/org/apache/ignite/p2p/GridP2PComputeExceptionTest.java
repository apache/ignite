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

package org.apache.ignite.p2p;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class GridP2PComputeExceptionTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_WITH_SER_EX =
        "org.apache.ignite.tests.p2p.compute.ExternalRunnableWithSerializableException";

    /** */
    private static final String RUN_WITH_EXT_EX =
        "org.apache.ignite.tests.p2p.compute.ExternalRunnableWithExternalizableException";

    /** */
    private static final String TASK_WITH_BROKEN_SERDES_EX =
        "org.apache.ignite.tests.p2p.compute.ExternalTaskWithBrokenExceptionSerialization";

    /** */
    private static final String SER_EX_CLS_NAME = "SerializableException";

    /** */
    private static final String EXT_EX_CLS_NAME = "ExternalizableException";

    /** */
    private static final String EXT_EX_WITH_BROKEN_SERDES_CLS_NAME = "ExternalizableExceptionWithBrokenSerialization";

    /** */
    public static final String EX_MSG = "Message from Exception";

    /** */
    public static final int EX_CODE = 127;

    /** */
    public static final String EX_DETAILS = "Details from Exception";

    /** */
    private static final String EX_BROKEN_SER_MSG = "Exception occurred on serialization step";

    /** */
    private static final String EX_REMOTE_JOB_MSG = "Remote job threw user exception";

    /** */
    private static final String EX_UNMARSHAL_MSG = "Failed to unmarshal object with optimized marshaller";

    /** */
    private static final String EX_DESERIALIZE_MSG = "Failed to deserialize object with given class loader";

    /** Test class loader. */
    private static final ClassLoader TEST_CLS_LDR;

    static {
        try {
            URL[] urls = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};

            TEST_CLS_LDR = new GridTestExternalClassLoader(urls);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPeerClassLoadingEnabled(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testComputeRunnableWithSerializableException() throws Exception {
        testComputeRunnableWithException(RUN_WITH_SER_EX, SER_EX_CLS_NAME);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testComputeRunnableWithExternalizableException() throws Exception {
        testComputeRunnableWithException(RUN_WITH_EXT_EX, EXT_EX_CLS_NAME);
    }

    /** */
    private void testComputeRunnableWithException(String runnableBinaryName, String exClsName) throws Exception {
        try (IgniteEx ignite = startGrids(3); IgniteEx cli = startClientGrid()) {
            Constructor<?> ctor = TEST_CLS_LDR.loadClass(runnableBinaryName).getConstructor();

            IgniteCompute compute = cli.compute(cli.cluster().forRemotes());

            assertThrows(() -> compute.run((IgniteRunnable)ctor.newInstance()), exClsName);

            assertEquals(4, ignite.cluster().topologyVersion());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBrokenExceptionSerialization() throws Exception {
        testComputeTaskWithBrokenException(TASK_WITH_BROKEN_SERDES_EX, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBrokenExceptionDeserialization() throws Exception {
        testComputeTaskWithBrokenException(TASK_WITH_BROKEN_SERDES_EX, false);
    }

    /**
     * @param taskBinaryName ComputeTask class binary name
     * @param isSerializationBroken {@code True} - if serialization step is broken,
     *                              {@code False} - if deserialization step is broken
     */
    @SuppressWarnings("unchecked")
    private void testComputeTaskWithBrokenException(String taskBinaryName, boolean isSerializationBroken) throws Exception {
        try (IgniteEx ignite = startGrids(3); IgniteEx cli = startClientGrid()) {
            IgniteCompute compute = cli.compute(cli.cluster().forRemotes());

            Constructor<?> ctor = TEST_CLS_LDR.loadClass(taskBinaryName).getConstructor(boolean.class);

            ComputeTask<Object, Object> task = (ComputeTask<Object, Object>)ctor.newInstance(isSerializationBroken);

            if (isSerializationBroken)
                compute.execute(task, null);
            else
                assertThrows(() -> compute.execute(task, null), EXT_EX_WITH_BROKEN_SERDES_CLS_NAME);

            assertEquals(4, ignite.cluster().topologyVersion());
        }
    }

    /**
     * Checks whether runnable throws expected exception or not.
     *
     * @param run Runnable.
     */
    private void assertThrows(RunnableX run, String clsName) throws NoSuchFieldException, IllegalAccessException {
        assert run != null;

        try {
            run.run();
        }
        catch (Throwable e) {
            Throwable ex = getCauseForRemoteJobException(e);

            if (clsName.equals(ex.getClass().getSimpleName())) {
                if (clsName.equals(EXT_EX_WITH_BROKEN_SERDES_CLS_NAME)) {
                    assertEquals(EX_BROKEN_SER_MSG, ex.getMessage());

                    return;
                }

                assertEquals(EX_MSG, ex.getMessage());
                assertNotNull(ex.getStackTrace());
                assertFieldEquals(ex, "code", EX_CODE);
                assertFieldEquals(ex, "details", EX_DETAILS);

                return;
            }

            throw new AssertionError("Unexpected exception has been thrown.", e);
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /** */
    private Throwable getCauseForRemoteJobException(Throwable e) {
        Throwable cause = e;

        while (cause.getCause() != null && skipToCause(e))
            cause = cause.getCause();

        return cause;
    }

    /** */
    private boolean skipToCause(Throwable ex) {
        return (ex.getMessage().contains(EX_REMOTE_JOB_MSG) ||
            ex.getMessage().contains(EX_UNMARSHAL_MSG) ||
            ex.getMessage().contains(EX_DESERIALIZE_MSG));
    }

    /** */
    private void assertFieldEquals(
        Throwable ex,
        String fieldName,
        Object expVal
    ) throws NoSuchFieldException, IllegalAccessException {
        Class<?> exCls = ex.getClass();

        Field detailsField = exCls.getDeclaredField(fieldName);
        detailsField.setAccessible(true);
        Object actVal = detailsField.get(ex);

        assertEquals(expVal, actVal);
    }
}
