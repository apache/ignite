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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.hamcrest.Matchers.is;

/**
 * Test for --classpath command.
 */
public class GridCommandHandlerClassPathTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(2);

        super.beforeTestsStarted();
    }

    /** Tests --create command. */
    @Test
    public void testCreate() throws Exception {
        Assume.assumeThat("Only cli mode supported", commandHandler, is(CLI_CMD_HND));

        //grid(0).cluster().state(ClusterState.ACTIVE);

        String jars = Files.list(Path.of(getClass().getClassLoader().getResource(".").getPath() + "../"))
            .map(Path::toAbsolutePath)
            .map(Path::toString)
            .filter(f -> f.endsWith("jar"))
            .collect(Collectors.joining(","));

        Path dir = Path.of(getClass().getClassLoader().getResource(".").getPath() + "../../../core/target");

        System.out.println("dir = " + dir);

        String coreJars = Files.list(dir)
            .map(Path::toAbsolutePath)
            .map(Path::toString)
            .filter(f -> f.endsWith("jar"))
            .collect(Collectors.joining(","));

        jars += "," + coreJars;

        injectTestSystemOut();

        final TestCommandHandler hnd = newCommandHandler(createTestLogger());

        try {
            assertEquals(EXIT_CODE_OK, execute(hnd, "--class-path", "create", "--name", "mysuperapp", "--files", jars));
        }
        finally {
            String outStr = testOut.toString();

            stopAllGrids();

            System.out.println(outStr);
        }
    }
    // TODO check empty file creation.
    // TODO add in production code checks of files integriy. Perform file integrity check on startup.
}
