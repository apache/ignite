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

package org.apache.ignite.tools.surefire.testsuites;

import java.util.Set;

/**
 * Assert if Ignite repository contains some orphaned (non-suited) tests.
 */
public class AssertOnOrphanedTests {
    /** @param args Command line arguments. */
    public static void main(String[] args) throws Exception {
        OrphanedTestCollection orphanedTestCollection = new OrphanedTestCollection();

        Set<String> orphanedTests = orphanedTestCollection.getOrphanedTests();

        if (orphanedTests.isEmpty()) {
            System.out.println("No issues with test and suites are found.");
            return;
        }

        orphanedTestCollection.persistOrphanedTests(orphanedTests, true);

        StringBuilder builder = new StringBuilder("All test classes must be include in any test suite")
            .append(" or mark with the @Ignore annotation.")
            .append("\nList of non-suited classes (")
            .append(orphanedTests.size())
            .append(" items):\n");

        for (String test : orphanedTests)
            builder.append("\t").append(test).append("\n");

        builder
            .append("\nList of orphaned tests persisted in: ")
            .append(orphanedTestCollection.getPath())
            .append("\n");

        AssertionError err = new AssertionError(builder.toString());
        err.setStackTrace(new StackTraceElement[] {});

        throw err;
    }
}
