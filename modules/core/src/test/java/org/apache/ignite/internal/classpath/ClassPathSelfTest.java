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
package org.apache.ignite.internal.classpath;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.classpath.ClassPathCreateCommand;
import org.apache.ignite.internal.management.classpath.ClassPathCreateCommandArg;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.classpath.ClassPathCreationFailoverTest.TIMEOUT;
import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.LOST;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ClassPathSelfTest extends GridCommonAbstractTest {
    // New node moves LOST to READY.

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testNewNodeDownloadExistingClassPathes() throws Exception {
        IgniteEx srv = startGrid(0);

        Set<Path> cpFiles = ClassPathTestUtils.files();

        ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

        arg.name("testcp");
        arg.files(ClassPathTestUtils.fileArg(cpFiles));

        new ClassPathCreateCommand().execute(null, grid(0), arg, l -> log.info(l));

        checkState(srv, "testcp");

        ClassPathTestUtils.checkDeployedOn(srv, "testcp");
        ClassPathTestUtils.checkFilesExists(srv, "testcp", cpFiles);

        IgniteEx srv1 = startGrid(1);

        ClassPathTestUtils.checkDeployedOn(srv1, "testcp");
        ClassPathTestUtils.checkFilesExists(grid(1), "testcp", cpFiles);
    }

    /** */
    @Test
    public void testNewNodeRemoveStaleOnStart() throws Exception {
        IgniteEx srv = startGrid(0);

        Set<Path> cpFiles = ClassPathTestUtils.files();

        ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

        arg.name("testcp");
        arg.files(ClassPathTestUtils.fileArg(cpFiles));

        new ClassPathCreateCommand().execute(null, grid(0), arg, l -> log.info(l));

        arg.name("testcp_v2");
        arg.files(ClassPathTestUtils.fileArg(cpFiles));

        new ClassPathCreateCommand().execute(null, grid(0), arg, l -> log.info(l));

        checkState(srv, "testcp");
        checkState(srv, "testcp_v2");

        ClassPathTestUtils.checkFilesExists(grid(0), "testcp", cpFiles);
        ClassPathTestUtils.checkFilesExists(grid(0), "testcp_v2", cpFiles);

        NodeFileTree ft = new NodeFileTree(new File(U.defaultWorkDirectory()), "second_node");

        IgniteClassPath stale = new IgniteClassPath(
            UUID.randomUUID(),
            Collections.emptySet(),
            "testcp_v2",
            new String[] {".stale"},
            new long[] {42},
            READY
        );

        IgniteClassPath unknown = new IgniteClassPath(
            UUID.randomUUID(),
            Collections.emptySet(),
            "unknown",
            new String[] {".unknown"},
            new long[] {42},
            READY
        );

        assertTrue(ft.classPathRoot(stale.name()).mkdirs());
        assertTrue(ft.classPathRoot(unknown.name()).mkdirs());

        ClassPathProcessor.writeClassPathDescriptor(ft, stale);
        ClassPathProcessor.writeClassPathDescriptor(ft, unknown);

        ListeningTestLogger lsnrLog = new ListeningTestLogger(log);

        LogListener rmvStaleLsnr = LogListener.matches("Stale local data. Removing " +
                "[loc=IgniteClassPath [id=" + stale.id() + ", name=" + stale.name() + ", state=" + stale.state() + "]")
            .times(1).build();

        LogListener rmvUnknownLsnr = LogListener.matches("Unknown local data. Removing " +
                "[icp=IgniteClassPath [id=" + unknown.id() + ", name=" + unknown.name() + ", state=" + unknown.state() + "]]")
            .times(1).build();

        lsnrLog.registerAllListeners(rmvStaleLsnr, rmvUnknownLsnr);

        IgniteEx srv1 = startGrid(getConfiguration(getTestIgniteInstanceName(1))
            .setConsistentId("second_node")
            .setGridLogger(lsnrLog));

        assertTrue(waitForCondition(rmvStaleLsnr::check, TIMEOUT));
        assertTrue(waitForCondition(rmvUnknownLsnr::check, TIMEOUT));

        ClassPathTestUtils.checkDeployedOn(srv, "testcp");
        ClassPathTestUtils.checkDeployedOn(srv1, "testcp_v2");

        assertNull(srv.context().distributedMetastorage().read(metastorageKey("unknown")));
    }

    /** */
    @Test
    public void testNewNodeMoveFromLostToReady() throws Exception {
        IgniteEx srv = startGrid(0);

        Set<Path> cpFiles = ClassPathTestUtils.files();

        int idx = 0;
        String[] names = new String[cpFiles.size()];
        long[] lengths = new long[cpFiles.size()];

        for (Path cpFile : cpFiles) {
            names[idx] = cpFile.getFileName().toString();
            lengths[idx] = Files.size(cpFile);
            idx++;
        }

        IgniteClassPath lost = new IgniteClassPath(
            UUID.randomUUID(),
            Collections.emptySet(),
            "testcp",
            names,
            lengths,
            LOST
        );

        assertTrue(srv.context().distributedMetastorage().compareAndSet(metastorageKey(lost.name()), null, lost));

        NodeFileTree ft = new NodeFileTree(new File(U.defaultWorkDirectory()), "second_node");

        File cpRoot = ft.classPathRoot(lost.name());

        assertTrue(cpRoot.mkdirs());
        assertTrue(ClassPathProcessor.guardFile(cpRoot, lost.id()).createNewFile());
        ClassPathProcessor.writeClassPathDescriptor(ft, lost);

        for (Path cpFile : cpFiles)
            Files.copy(cpFile, new File(cpRoot, cpFile.getFileName().toString()).toPath());

        ListeningTestLogger lsnrLog = new ListeningTestLogger(log);

        IgniteEx srv1 = startGrid(getConfiguration(getTestIgniteInstanceName(1))
            .setConsistentId("second_node")
            .setGridLogger(lsnrLog));

        ClassPathTestUtils.checkDeployedOn(srv, "testcp");
        ClassPathTestUtils.checkDeployedOn(srv1, "testcp");

        ClassPathTestUtils.checkFilesExists(srv, "testcp", cpFiles);
        ClassPathTestUtils.checkFilesExists(srv1, "testcp", cpFiles);

        assertEquals(READY, srv.context().distributedMetastorage().<IgniteClassPath>read(metastorageKey(lost.name())).state());
    }

    /** */
    private static void checkState(IgniteEx srv, String testcp_v2) throws IgniteCheckedException {
        IgniteClassPath icp = srv.context().distributedMetastorage().read(metastorageKey(testcp_v2));

        assertNotNull(icp);
        assertEquals(READY, icp.state());
        assertTrue(icp.deployedOnNodes().contains(srv.localNode().id()));
    }
}
