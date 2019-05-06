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

package org.apache.ignite.internal.processors.diagnostic;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.DiagnosticMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.diagnostic.DebugProcessor.DEFAULT_TARGET_FOLDER;

/** */
public class DiagnosticMXBeanTest extends GridCommonAbstractTest {
    /** **/
    private static final String TEST_DUMP_FILE = "custom";

    /** One time instantiated debug bean for test. **/
    private static DiagnosticMXBean debugBean;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, false));

        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        debugBean = debugMxBean();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldDumpInfoOnlyToLog() throws Exception {
        debugBean.dumpPageHistory(false, true, 2323);

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);

        assertEquals(0, path.toFile().listFiles((dir, name) -> name.endsWith("txt")).length);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldDumpInfoOnlyToLogAndFile() throws Exception {
        long[] expectedPages = {2323, 44564, 456456};
        debugBean.dumpPageHistory(true, true, expectedPages);

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);

        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith("txt"))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(records.size() > 0);

        assertTrue(records.stream().anyMatch(line -> line.contains("CheckpointRecord")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldDumpInfoOnlyToCustomFile() throws Exception {
        debugBean.dumpPageHistory(true, false, TEST_DUMP_FILE, 49984);

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, TEST_DUMP_FILE);

        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith("txt"))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(records.size() > 0);

        assertTrue(records.stream().anyMatch(line -> line.contains("CheckpointRecord")));
    }

    /**
     *
     */
    private DiagnosticMXBean debugMxBean() throws Exception {
        ObjectName mBeanName = U.makeMBeanName(getTestIgniteInstanceName(), "Debug",
            DiagnosticMXBeanImpl.class.getSimpleName());

        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        assertTrue(mBeanSrv.isRegistered(mBeanName));

        Class<DiagnosticMXBean> itfCls = DiagnosticMXBean.class;

        return MBeanServerInvocationHandler.newProxyInstance(mBeanSrv, mBeanName, itfCls, true);
    }
}
