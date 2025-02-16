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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerClusterByClassTest.dumpFileNameMatcher;

/** */
public class IdleVerifyDumpTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setWorkDirectory(nodeWorkDirectory(igniteInstanceName));
    }

    /**
     * Test ensuring that idle verify dump output file is created exactly
     * on server node specified via the --host parameter.
     */
    @Test
    public void testDumpResultMatchesConnection() throws Exception {
        injectTestSystemOut();

        client.createCache(DEFAULT_CACHE_NAME).put(1, 1);

        checkDump(0);

        checkDump(1);
    }

    /** */
    private void checkDump(int nodeIdx) throws Exception {
        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--port", connectorPort(grid(nodeIdx))));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        assertTrue(fileNameMatcher.find());

        Path dumpFileName = Paths.get(fileNameMatcher.group(1));

        String dumpRes = new String(Files.readAllBytes(dumpFileName));

        assertContains(log, dumpRes, "The check procedure has finished, no conflicts have been found.");

        assertContains(log, dumpFileName.toString(), nodeWorkDirectory(getTestIgniteInstanceName(nodeIdx)));
    }

    /** */
    private String nodeWorkDirectory(String igniteInstanceName) throws IgniteCheckedException {
        return new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath();
    }
}
