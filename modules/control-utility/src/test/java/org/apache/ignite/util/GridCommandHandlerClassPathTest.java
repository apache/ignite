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

package org.apache.ignite.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.classpath.ClassPathProcessor;
import org.apache.ignite.internal.classpath.ClassPathTestUtils;
import org.apache.ignite.internal.classpath.IgniteClassPath;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;
import static org.apache.ignite.internal.classpath.ClassPathTestUtils.file;
import static org.apache.ignite.internal.classpath.ClassPathTestUtils.fileNames;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test for --classpath command.
 */
public class GridCommandHandlerClassPathTest extends GridCommandHandlerAbstractTest {
    /** */
    public static final int GRID_CNT = 3;

    /** */
    public static final int FAIL_NODE_IDX = 2;

    /** */
    private Set<Path> cpFiles;

    /** */
    private String filesArg;

    /** */
    private ListeningTestLogger lsnrLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setGridLogger(lsnrLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cpFiles = ClassPathTestUtils.files();

        filesArg = String.join(",", ClassPathTestUtils.fileArg(cpFiles));

        cleanPersistenceDir();

        lsnrLog = new ListeningTestLogger(log);

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    // TODO: add CRC or other check of file integrity.
    // TODO add in production code checks of files integriy. Perform file integrity check on startup.

    // Support pretty print for command.
    // TODO: add MAX file size check, free disk amount check.

    /** Concurrent {@code --create} commands with the same name must result in exactly one success. */
    @Test
    public void testConcurrentCreateSameName() throws Exception {
        injectTestSystemOut();

        int cmdCnt = 4;

        LogListener cpReadyLsnr = readyLogListener(GRID_CNT - 1);

        lsnrLog.registerListener(cpReadyLsnr);

        List<Set<Path>> fileSets = new ArrayList<>();

        for (int i = 0; i < cmdCnt; i++)
            fileSets.add(Set.of(file(1000L * (i + 1) + i)));

        TestCommandHandler[] hnds = new TestCommandHandler[cmdCnt];

        for (int i = 0; i < cmdCnt; i++)
            hnds[i] = newCommandHandler(createTestLogger());

        AtomicInteger okCnt = new AtomicInteger();
        AtomicInteger failCnt = new AtomicInteger();
        AtomicInteger idx = new AtomicInteger();

        Set<Path> okFiles = new HashSet<>();

        CountDownLatch latch = new CountDownLatch(cmdCnt);

        GridTestUtils.runMultiThreadedAsync(() -> {
            int i = idx.getAndIncrement();

            latch.countDown();

            assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

            int res = execute(
                hnds[i],
                "--class-path", "create",
                "--name", cpName(),
                "--files", String.join(",", ClassPathTestUtils.fileArg(fileSets.get(i)))
            );

            if (res == EXIT_CODE_OK) {
                okCnt.incrementAndGet();

                okFiles.addAll(fileSets.get(i));
            }
            else
                failCnt.incrementAndGet();

            return null;
        }, cmdCnt, "concurrent-cp-create").get(getTestTimeout());

        assertEquals(1, okCnt.get());
        assertEquals(cmdCnt - 1, failCnt.get());
        assertTrue(waitForCondition(cpReadyLsnr::check, 30_000));

        IgniteClassPath icp = classPath();

        assertNotNull(icp);
        assertEquals(READY, icp.state());
        assertEquals(GRID_CNT, icp.deployedOnNodes().size());

        assertEquals(fileNames(okFiles), new HashSet<>(Arrays.asList(icp.files())));

        checkFilesExists(cpName(), -1, okFiles);
    }

    /** Tests --create command. */
    @Test
    public void testCreate() throws Exception {
        injectTestSystemOut();

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        // Empty root doesn't affect ClassPath creation.
        if (commandHandler.equals(JMX_CMD_HND))
            assertTrue(grid(0).context().pdsFolderResolver().fileTree().classPathRoot(cpName()).mkdirs());

        LogListener cpReadyLsnr = readyLogListener(GRID_CNT - 1);

        lsnrLog.registerListener(cpReadyLsnr);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--class-path", "create", "--name", cpName(), "--files", filesArg));

        assertTrue(waitForCondition(cpReadyLsnr::check, 30_000));

        IgniteClassPath icp = classPath();

        assertEquals(READY, icp.state());
        assertEquals(GRID_CNT, icp.deployedOnNodes().size());

        ClassPathTestUtils.checkDeployedOn(grid(0), cpName());
        ClassPathTestUtils.checkDeployedOn(grid(1), cpName());
        ClassPathTestUtils.checkDeployedOn(grid(2), cpName());

        checkFilesExists(cpName(), -1, cpFiles);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--class-path", "remove", "--name", cpName()));
    }

    /** Tests --create command. */
    @Test
    public void testCreateWhenRootExists() throws Exception {
        injectTestSystemOut();

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        File cpRoot = grid(FAIL_NODE_IDX).context().pdsFolderResolver().fileTree().classPathRoot(cpName());

        assertTrue(cpRoot.mkdirs());

        File f = new File(cpRoot, ".must_fail_cp_creation");

        assertTrue(f.createNewFile());

        LogListener downloadSucceedMsg = readyLogListener(1);

        lsnrLog.registerListener(downloadSucceedMsg);

        assertEquals(EXIT_CODE_OK, execute(hnd, "--class-path", "create", "--name", cpName(), "--files", filesArg));

        assertTrue(waitForCondition(downloadSucceedMsg::check, 30_000));

        IgniteClassPath icp = classPath();

        assertEquals(READY, icp.state());
        assertEquals(2, icp.deployedOnNodes().size());

        ClassPathTestUtils.checkDeployedOn(grid(0), cpName());
        ClassPathTestUtils.checkDeployedOn(grid(1), cpName());

        checkFilesExists(cpName(), FAIL_NODE_IDX, cpFiles);
    }

    /** */
    @Test
    public void testFailWhenFileExists() throws Exception {
        injectTestSystemOut();

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        GridKernalContext ctx = grid(0).context();

        ctx.distributedMetastorage().listen(
            k -> k.equals(metastorageKey(cpName())),
            (key, oldVal, newVal) ->
                fileNames(cpFiles).forEach(f -> new File(ctx.pdsFolderResolver().fileTree().classPathRoot(cpName()), f).mkdirs())
        );

        LogListener cpReadyLsnr = null;

        if (!cliCommandHandler()) {
            cpReadyLsnr = LogListener
                .matches("Failed to copy ClassPath file locally, the ClassPath will be removed")
                .times(1)
                .build();

            lsnrLog.registerListener(cpReadyLsnr);
        }

        assertEquals(
            EXIT_CODE_UNEXPECTED_ERROR,
            execute(hnd, "--class-path", "create", "--name", cpName(), "--files", filesArg)
        );

        String out = testOut.toString();

        if (cliCommandHandler())
            assertTrue(out.contains("Starting to upload files:"));
        else
            assertTrue(waitForCondition(cpReadyLsnr::check, 30_000));

        assertTrue(out.contains("File exists"));
        assertNull("Metastorage record must be removed", classPath());
        assertFalse(
            "Classpath directory must be removed",
            grid(0).context().pdsFolderResolver().fileTree().classPathRoot(cpName()).exists()
        );
    }

    /** */
    @Test
    public void testFailWhenMetastoreExists() throws Exception {
        injectTestSystemOut();

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        assertTrue(grid(0).context().distributedMetastorage().compareAndSet(
            metastorageKey(cpName()),
            null,
            1
        ));

        assertEquals(
            EXIT_CODE_UNEXPECTED_ERROR,
            execute(hnd, "--class-path", "create", "--name", cpName(), "--files", filesArg)
        );

        String out = testOut.toString();

        assertTrue(out.contains("Fail to register ClassPath. Same ClassPath exists, already?"));
        assertNotNull(grid(0).context().distributedMetastorage().read(ClassPathProcessor.metastorageKey(cpName())));
        assertFalse(
            "Classpath directory must be removed",
            grid(0).context().pdsFolderResolver().fileTree().classPathRoot(cpName()).exists()
        );
        assertTrue(
            "Files must not be copied",
            F.isEmpty(grid(0).context().pdsFolderResolver().fileTree().classPathRoot(cpName()).listFiles())
        );
    }

    /** Tests --create command arguments format. */
    @Test
    public void testEmptyFilesArgument() {
        injectTestSystemOut();

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, "--class-path", "create", "--name", "mysuperapp", "--files"),
            "Please specify a value for argument: --files"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, "--class-path", "create", "--name", "mysuperapp"),
            "Mandatory argument(s) missing: [--files]"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, "--class-path", "create", "--name", "--files", "some_files"),
            "Please specify a value for argument: --name"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, "--class-path", "create", "--files", "some_files"),
            "Mandatory argument(s) missing: [--name]"
        );

        assertContains(
            log,
            executeCommand(
                EXIT_CODE_INVALID_ARGUMENTS,
                "--class-path", "create", "--name", "mysuperapp", "--files", ""
            ),
            cliCommandHandler() ? "File name must not be empty" : "Argument --files required"
        );

        assertContains(
            log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, "--class-path", "create", "--files", "f.txt,f.txt"),
            "Mandatory argument(s) missing: [--name]"
        );
    }

    /** */
    private void checkFilesExists(String cpName, int skip, Set<Path> files) throws IOException, IgniteCheckedException {
        for (int i = 0; i < GRID_CNT; i++) {
            if (skip == i)
                continue;

            ClassPathTestUtils.checkFilesExists(grid(i), cpName, files);
        }
    }

    /** */
    private IgniteClassPath classPath() throws IgniteCheckedException {
        return grid(0).context().distributedMetastorage().read(ClassPathProcessor.metastorageKey(cpName()));
    }

    /** */
    private static LogListener readyLogListener(int succeed) {
        return LogListener
            .matches("IgniteClassPath task done [task=download")
            .times(succeed)
            .build();
    }

    /** */
    private String cpName() {
        return "mysuperapp_" + commandHandler;
    }
}
