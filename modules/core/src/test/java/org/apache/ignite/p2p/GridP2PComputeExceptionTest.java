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
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class GridP2PComputeExceptionTest extends GridCommonAbstractTest {
    /** */
    private static final String RUNNABLE_WITH_SERIALIZABLE_EXCEPTION =
        "org.apache.ignite.tests.p2p.compute.ExternalRunnableWithSerializableException";

    /** */
    private static final String RUNNABLE_WITH_EXTERNALIZABLE_EXCEPTION =
        "org.apache.ignite.tests.p2p.compute.ExternalRunnableWithExternalizableException";

    /** */
    private static final String TASK_WITH_BROKEN_EXCEPTION =
        "org.apache.ignite.tests.p2p.compute.ExternalTaskWithBrokenExceptionSerialization";

    /** */
    public static final String EX_MSG = "Message from Exception";

    /** */
    private static final String EX_UNMARSHAL_MSG = "Failed to unmarshal object with optimized marshaller";

    /** */
    private static final String EX_SERIALIZE_MSG = "Failed to serialize job exception";

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
        testComputeRunnableWithException(RUNNABLE_WITH_SERIALIZABLE_EXCEPTION);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testComputeRunnableWithExternalizableException() throws Exception {
        testComputeRunnableWithException(RUNNABLE_WITH_EXTERNALIZABLE_EXCEPTION);
    }

    /** */
    private void testComputeRunnableWithException(String runnableBinaryName) throws Exception {
        try (IgniteEx ignite = startGrids(3); IgniteEx cli = startClientGrid()) {
            Object runnable = TEST_CLS_LDR.loadClass(runnableBinaryName).getConstructor().newInstance();
            IgniteCompute compute = cli.compute(cli.cluster().forRemotes());

            assertThrows(log, () -> compute.run((IgniteRunnable)runnable), IgniteException.class, EX_MSG);
            assertEquals(4, ignite.cluster().topologyVersion());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBrokenExceptionSerialization() throws Exception {
        testComputeTaskWithBrokenException(TASK_WITH_BROKEN_EXCEPTION, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBrokenExceptionDeserialization() throws Exception {
        testComputeTaskWithBrokenException(TASK_WITH_BROKEN_EXCEPTION, false);
    }

    /**
     * @param taskBinaryName ComputeTask class binary name
     * @param isWriteBroken {@code True} - if serialization step is broken,
     *                      {@code False} - if deserialization step is broken
     */
    @SuppressWarnings("unchecked")
    private void testComputeTaskWithBrokenException(String taskBinaryName, boolean isWriteBroken) throws Exception {
        try (IgniteEx ignite = startGrids(3); IgniteEx cli = startClientGrid()) {
            Constructor<?> ctor = TEST_CLS_LDR.loadClass(taskBinaryName).getConstructor(boolean.class);
            ComputeTask<Object, Object> task = (ComputeTask<Object, Object>)ctor.newInstance(isWriteBroken);

            IgniteCompute compute = cli.compute(cli.cluster().forRemotes());

            if (isWriteBroken)
                assertThrows(log, () -> compute.execute(task, null), IgniteException.class, EX_SERIALIZE_MSG);
            else
                assertThrows(log, () -> compute.execute(task, null), BinaryObjectException.class, EX_UNMARSHAL_MSG);

            assertEquals(4, ignite.cluster().topologyVersion());
        }
    }
}
