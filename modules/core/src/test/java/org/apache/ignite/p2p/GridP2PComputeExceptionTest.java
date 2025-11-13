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
import java.net.URL;
import org.apache.ignite.IgniteCompute;
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
    private static final String RUN_WITH_SERIALIZABLE_EX =
        "org.apache.ignite.tests.p2p.compute.ExternalRunnableWithSerializableException";

    /** */
    private static final String RUN_WITH_EXTERNALIZABLE_EX =
        "org.apache.ignite.tests.p2p.compute.ExternalRunnableWithExternalizableException";

    /** */
    private static final String SERIALIZABLE_EXCEPTION_CLS_NAME = "SerializableException";

    /** */
    private static final String EXTERNALIZABLE_EXCEPTION_CLS_NAME = "ExternalizableException";

    /** */
    private static final String MSG = "Message from Exception";

    /** */
    private static final int CODE = 127;

    /** */
    private static final String DETAILS = "Details from Exception";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSerializableException() throws Exception {
        testException(RUN_WITH_SERIALIZABLE_EX, SERIALIZABLE_EXCEPTION_CLS_NAME);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testExternalizableException() throws Exception {
        testException(RUN_WITH_EXTERNALIZABLE_EX, EXTERNALIZABLE_EXCEPTION_CLS_NAME);
    }

    /** */
    private void testException(String runnableBinaryName, String exClsName) throws Exception {
        try (IgniteEx ignite = startGrid(0); IgniteEx cli = startClientGrid()) {
            IgniteCompute computeForRemotes = cli.compute(ignite.cluster().forRemotes());

            ClassLoader testClsLdr = new GridTestExternalClassLoader(
                new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))}
            );

            Constructor<?> ctor = testClsLdr.loadClass(runnableBinaryName).getConstructor();

            assertThrows(() -> computeForRemotes.run((IgniteRunnable)ctor.newInstance()), exClsName);
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
                assertEquals(MSG, ex.getMessage());
                assertNotNull(ex.getStackTrace());
                assertFieldEquals(ex, "code", CODE);
                assertFieldEquals(ex, "details", DETAILS);

                return;
            }

            throw new AssertionError("Unexpected exception has been thrown.", e);
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /** */
    private Throwable getCauseForRemoteJobException(Throwable e) {
        Throwable cause = e;

        while (cause.getCause() != null && cause.getMessage().contains("Remote job threw user exception"))
            cause = cause.getCause();

        return cause;
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
