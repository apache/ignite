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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.classpath.IgniteClassPath;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.junit.Test;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.METASTORE_PREFIX;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Test for --classpath command.
 */
public class GridCommandHandlerClassPathTest extends GridCommandHandlerAbstractTest {
    /** */
    public static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(GRID_CNT);

        super.beforeTestsStarted();
    }

    // TODO: add test that classpath can be created only with ADMIN_OPS privelege.
    // TODO: add CRC or other check of file integrity.
    // TODO add in production code checks of files integriy. Perform file integrity check on startup.
    // Support pretty print for command.
    // Test for different failure types: node fail, file write fail, etc.
    // Node fail: upload node vs not upload node.
    // Check cleanup.
    // Concurrent creation of CP with the same name.

    /** Tests --create command. */
    @Test
    public void testCreate() throws Exception {
        File emptyFile = File.createTempFile("empty", ".txt");

        emptyFile.deleteOnExit();

        Set<Path> cpFiles = files(
            Path.of(getClass().getClassLoader().getResource(".").getPath() + "../"),
            Path.of(getClass().getClassLoader().getResource(".").getPath() + "../../../core/target"),
            emptyFile.toPath()
        );

        injectTestSystemOut();

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        String cpName = "mysuperapp_" + commandHandler;
        String files = cpFiles.stream().map(Path::toFile).map(File::getAbsolutePath).collect(Collectors.joining(","));

        assertEquals(EXIT_CODE_OK, execute(hnd, "--class-path", "create", "--name", cpName, "--files", files));

        IgniteClassPath icp = classPath(cpName);

        assertEquals(READY, icp.state());

        Set<String> cpFilesNames = fileNames(cpFiles);

        checkFilesExists(cpFilesNames, cpName);

        try {
            // Attemp to create ClassPath with the same name must fail.
            assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(hnd, "--class-path", "create", "--name", cpName, "--files", files));
        }
        finally {
            assertTrue(testOut.toString().contains("Fail to register ClassPath. Same ClassPath exists, already?"));

            assertEquals("Metastorage state must not change", icp, classPath(cpName));

            checkFilesExists(cpFilesNames, cpName);
        }
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
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, "--class-path", "create", "--name", "mysuperapp", "--files", ""),
            cliCommandHandler() ? "File name must not be empty" : "Argument --files required"
        );
    }

    /** */
    private Set<String> fileNames(Set<Path> dirs) {
        return dirs.stream().map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
    }

    /** */
    private Set<Path> files(Path... paths) {
        return Stream.of(paths).flatMap(path -> {
            try {
                return Files.isDirectory(path)
                    ? Files.list(path).filter(p -> p.getFileName().toString().endsWith("jar") || p.getFileName().toString().endsWith("txt"))
                    : Stream.of(path);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).map(Path::toAbsolutePath).collect(Collectors.toSet());
    }

    /** */
    private void checkFilesExists(Set<String> cpFilesNames, String cpName) {
        for (int i = 0; i < GRID_CNT; i++) {
            NodeFileTree ft = grid(i).context().pdsFolderResolver().fileTree();

            assertEquals("Files must be deployed on each node", cpFilesNames, fileNames(files(ft.classPathRoot(cpName).toPath())));
        }
    }

    /** */
    private IgniteClassPath classPath(String cpName) throws IgniteCheckedException {
        return grid(0).context().distributedMetastorage().read(METASTORE_PREFIX + cpName);
    }
}
