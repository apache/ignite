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

package org.apache.ignite.example.sql.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.ignite.example.ExampleTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * These tests check that all SQL JDBC examples pass correctly.
 */
public class SqlExamplesTest {
    /** Empty argument to invoke an example. */
    protected static final String[] EMPTY_ARGS = new String[0];

    /**
     * Runs SqlJdbcExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlJdbcExample() throws Exception {
        ExampleTestUtils.assertConsoleOutputContains(SqlJdbcExample::main, EMPTY_ARGS,
            ">>> Query results:\n" +
            ">>>    John, Doe, Forest Hill\n" +
            ">>>    Jane, Roe, Forest Hill\n" +
            ">>>    Mary, Major, Denver\n" +
            ">>>    Richard, Miles, St. Petersburg\n",

            ">>> Query results:\n" +
            ">>>    John, Doe, 1000.0\n" +
            ">>>    Richard, Miles, 1450.0\n",

            ">>> Query results:\n" +
            ">>>    Jane, Roe, Forest Hill\n" +
            ">>>    Mary, Major, Denver\n" +
            ">>>    Richard, Miles, St. Petersburg\n"
        );
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
