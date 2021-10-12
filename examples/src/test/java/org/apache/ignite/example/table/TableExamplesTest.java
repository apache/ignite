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

package org.apache.ignite.example.table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.ignite.IgnitionManager;
import org.apache.ignite.example.ExampleTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * These tests check that all table examples pass correctly.
 */
public class TableExamplesTest {
    /** Empty argument to invoke an example. */
    protected static final String[] EMPTY_ARGS = new String[0];

    /**
     * Runs RecordViewExample.
     *
     * @throws Exception If failed and checks its output.
     */
    @Test
    public void testRecordViewExample() throws Exception {
        ExampleTestUtils.assertConsoleOutputContains(RecordViewExample::main, EMPTY_ARGS,
            "\nRetrieved record:\n" +
            "    Account Number: 123456\n" +
            "    Owner: Val Kulichenko\n" +
            "    Balance: $100.0\n");
    }

    /**
     * Runs KeyValueViewExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKeyValueViewExample() throws Exception {
        ExampleTestUtils.assertConsoleOutputContains(KeyValueViewExample::main, EMPTY_ARGS,
            "\nRetrieved value:\n" +
            "    Account Number: 123456\n" +
            "    Owner: Val Kulichenko\n" +
            "    Balance: $100.0\n");
    }

    @BeforeEach
    private void startNode() throws IOException {
        Path workDir = Path.of("my-first-node-work");

        if (Files.exists(workDir))
            IgniteUtils.deleteIfExists(workDir);

        IgnitionManager.start(
            "my-first-node",
            Files.readString(Path.of("config", "ignite-config.json")),
            workDir
        );
    }

    @AfterEach
    private void stopNode() {
        IgnitionManager.stop("my-first-node");

        Path workDir = Path.of("my-first-node-work");

        if (Files.exists(workDir))
            IgniteUtils.deleteIfExists(workDir);
    }

    /**
     * Removes a previously created work directory.
     */
    @BeforeEach
    @AfterEach
    private void removeWorkDir() {
        Path workDir = Path.of("work");

        if (Files.exists(workDir))
            IgniteUtils.deleteIfExists(workDir);
    }
}
