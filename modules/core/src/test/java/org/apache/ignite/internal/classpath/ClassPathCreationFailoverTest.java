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
import java.nio.file.Path;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.classpath.ClassPathCreateCommand;
import org.apache.ignite.internal.management.classpath.ClassPathCreateCommandArg;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.LOST;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ClassPathCreationFailoverTest extends GridCommonAbstractTest {
    /** */
    public static final int TIMEOUT = 10_000;

    /** */
    private static IgniteEx failedNode;

    /** */
    private static boolean sendMessage;

    /** */
    private static Runnable callback;

    /** */
    private ListeningTestLogger lsnrLog;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        lsnrLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        failedNode = null;

        sendMessage = false;

        callback = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(lsnrLog)
            .setCommunicationSpi(new TcpCommunicationSpi() {
                @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                    if (msg instanceof GridIoMessage
                        && ((GridIoMessage)msg).message() instanceof DownloadClassPathMessage
                        && !failedNode.localNode().id().equals(ignite.cluster().localNode().id())
                    ) {
                        if (callback != null)
                            callback.run();

                        if (!sendMessage)
                            return;
                    }

                    super.sendMessage(node, msg, ackC);
                }
            });
    }

    // TODO: add stopped check during async operations.
    // TODO: don't create on client node. ???

    /** */
    @Test
    public void testUploadNodeFailDuringDeployment() throws Exception {
        failedNode = startGrid(0);

        callback = () -> GridTestUtils.runAsync(() -> failedNode.close());

        IgniteEx grid1 = startGrid(1);

        awaitPartitionMapExchange();

        Set<Path> cpFiles = ClassPathTestUtils.files();

        LogListener createdMsg = newClassPathListener();
        LogListener uploadNodeFailedMsg = logListener("Failed to download ClassPath files [icp=testcp]");

        lsnrLog.registerAllListeners(createdMsg, uploadNodeFailedMsg);

        ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

        arg.name("testcp");
        arg.files(ClassPathTestUtils.fileArg(cpFiles));

        // ClassPath must be created on `failedNode`. Because, it first in cluster.
        new ClassPathCreateCommand().execute(null, grid(0), arg, l -> failedNode.log().info(l));

        assertTrue(waitForCondition(createdMsg::check, TIMEOUT));
        assertTrue(waitForCondition(() -> failedNode.context().isStopping(), TIMEOUT));

        stopGrid(0);

        assertEquals(1, Ignition.allGrids().size());

        assertTrue(waitForCondition(() -> {
            Object t = getFieldValue(ClassPathTestUtils.transmissionHandler(grid1), "active");

            if (t == null)
                return true;

            GridFutureAdapter<?> res = getFieldValue(t, "res");

            return res.isDone();
        }, TIMEOUT));

        assertTrue(waitForCondition(uploadNodeFailedMsg::check, TIMEOUT));
        assertTrue(waitForCondition(() -> {
                try {
                    return grid1.context().distributedMetastorage().<IgniteClassPath>read(metastorageKey("testcp")).state() == LOST;
                }
                catch (IgniteCheckedException e) {
                    return false;
                }
            },
            TIMEOUT
        ));
        assertTrue(waitForCondition(
            () -> !grid1.context().pdsFolderResolver().fileTree().classPathRoot("testcp").exists(),
            TIMEOUT
        ));

        IgniteClassPath icp = grid1.context().distributedMetastorage().read(metastorageKey("testcp"));

        assertNotNull(icp);
        assertEquals(LOST, icp.state());
        assertTrue(icp.deployedOnNodes().isEmpty());
    }

    /** */
    @Test
    public void testDownloadNodeFailDuringDeployment() throws Exception {
        startGrids(3);

        callback = () -> GridTestUtils.runAsync(() -> failedNode.close());

        failedNode = grid(1);
        sendMessage = true;

        awaitPartitionMapExchange();

        Set<Path> cpFiles = ClassPathTestUtils.files();

        LogListener createdMsg = newClassPathListener();
        LogListener oneNodeFailMsg = logListener("IgniteClassPath task failure [task=Download files");
        LogListener donloadSucceedMsg = logListener("IgniteClassPath task done [task=Download files");

        lsnrLog.registerAllListeners(createdMsg, oneNodeFailMsg, donloadSucceedMsg);

        ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

        arg.name("testcp");
        arg.files(ClassPathTestUtils.fileArg(cpFiles));

        // ClassPath must be created on `failedNode`. Because, it first in cluster.
        new ClassPathCreateCommand().execute(null, grid(0), arg, l -> failedNode.log().info(l));

        assertTrue(waitForCondition(createdMsg::check, TIMEOUT));
        assertTrue(waitForCondition(() -> failedNode.context().isStopping(), TIMEOUT));

        stopGrid(1);

        assertEquals(2, Ignition.allGrids().size());

        assertTrue(waitForCondition(oneNodeFailMsg::check, TIMEOUT));
        assertTrue(waitForCondition(donloadSucceedMsg::check, TIMEOUT));

        ClassPathTestUtils.checkFilesExists(grid(0), "testcp", cpFiles);
        ClassPathTestUtils.checkFilesExists(grid(2), "testcp", cpFiles);

        IgniteClassPath icp = grid(0).context().distributedMetastorage().read(ClassPathProcessor.metastorageKey("testcp"));

        assertNotNull(icp);
        assertEquals(READY, icp.state());
        assertEquals(2, icp.deployedOnNodes().size());

        ClassPathTestUtils.checkDeployedOn(grid(0), "testcp");
        ClassPathTestUtils.checkDeployedOn(grid(2), "testcp");
    }

    /** */
    @Test
    public void testConcurrentFileCreationDuringDeployment() throws Exception {
        startGrids(2);

        sendMessage = true;
        failedNode = grid(0);

        awaitPartitionMapExchange();

        Set<Path> cpFiles = ClassPathTestUtils.files();

        callback = () -> {
            File cpRoot = grid(1).context().pdsFolderResolver().fileTree().classPathRoot("testcp");

            ClassPathTestUtils.fileNames(cpFiles).forEach(f -> new File(cpRoot, f).mkdirs());
        };

        LogListener createdMsg = newClassPathListener();
        LogListener oneNodeFailMsg = logListener("Failed to download ClassPath files [icp=testcp]");

        lsnrLog.registerAllListeners(createdMsg, oneNodeFailMsg);

        ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

        arg.name("testcp");
        arg.files(ClassPathTestUtils.fileArg(cpFiles));

        // ClassPath must be created on `failedNode`. Because, it first in cluster.
        new ClassPathCreateCommand().execute(null, grid(0), arg, l -> grid(0).log().info(l));

        assertTrue(waitForCondition(createdMsg::check, TIMEOUT));

        assertEquals(2, Ignition.allGrids().size());

        assertTrue(waitForCondition(oneNodeFailMsg::check, TIMEOUT));

        ClassPathTestUtils.checkFilesExists(grid(0), "testcp", cpFiles);

        IgniteClassPath icp = grid(0).context().distributedMetastorage().read(metastorageKey("testcp"));

        assertNotNull(icp);
        assertEquals(READY, icp.state());
        assertEquals(1, icp.deployedOnNodes().size());

        ClassPathTestUtils.checkDeployedOn(grid(0), "testcp");

        assertFalse(grid(1).context().pdsFolderResolver().fileTree().classPathRoot("testcp").exists());
    }

    /** */
    private LogListener newClassPathListener() {
        return logListener("New ClassPath created [uploadNode=" + grid(0).localNode().id());
    }

    /** */
    private static LogListener logListener(String msg) {
        return LogListener.matches(msg).times(1).build();
    }
}
