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

package org.apache.ignite.internal.runner.app;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import org.apache.ignite.app.IgniteCliRunner;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests the start ignite nodes.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class IgniteCliRunnerTest {
    @WorkDirectory
    private Path workDir;

    /** TODO: Replace this test by full integration test on the cli side IGNITE-15097. */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15729")
    public void runnerArgsSmokeTest() {
        assertNotNull(IgniteCliRunner.start(
            new String[] {
                "--config", workDir.resolve("node1").toAbsolutePath().toString(),
                "--work-dir", workDir.resolve("node1").toAbsolutePath().toString(),
                "node1"
            }
        ));

        assertNotNull(IgniteCliRunner.start(
            new String[] {
                "--work-dir", workDir.resolve("node2").toAbsolutePath().toString(),
                "node2"
            }
        ));
    }
}
