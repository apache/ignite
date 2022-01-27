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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a persisted list of orphaned tests.
 */
public class OrphanedTestCollection {
    /**
     * This line in the file shows that the list of orphaned tests is final for all maven modules.
     * {@link #getOrphanedTests()} ignores a content of the file if read this mark.
     * */
    private static final String FINAL_MARK = "---";

    /** File to persist orphaned tests. */
    private final Path path = initPath();

    /** @return {@link Set} of orphaned test names. */
    public Set<String> getOrphanedTests() throws Exception {
        if (Files.notExists(path))
            return new HashSet<>();

        try (
            BufferedReader testReader = new BufferedReader(new FileReader(path.toFile()))
        ) {
            String testClsName = testReader.readLine();

            if (FINAL_MARK.equals(testClsName))
                return new HashSet<>();

            Set<String> testClasses = new HashSet<>();

            while (testClsName != null) {
                testClasses.add(testClsName);

                testClsName = testReader.readLine();
            }

            return testClasses;
        }
    }

    /**
     * @param testClasses Collection of test classes names.
     * @param last Whether it's the last call within whole project.
     */
    public void persistOrphanedTests(Collection<String> testClasses, boolean last) throws Exception {
        try (
            BufferedWriter testWriter = new BufferedWriter(new FileWriter(path.toFile()))
        ) {
            if (last) {
                testWriter.write(FINAL_MARK);
                testWriter.newLine();
            }

            for (String cls: testClasses) {
                testWriter.write(cls);
                testWriter.newLine();
            }
        }
    }

    /** @return Path of the file to persist orphaned tests into. */
    public Path getPath() {
        return path;
    }

    /**
     * Structure of Ignite modules is flat but there are some exceptions. Unfortunately it's impossible to
     * get access to root directory of repository so use this hack to find it.
     */
    private static Path initPath() {
        Path curPath = Paths.get("").toAbsolutePath();

        while (!curPath.equals(curPath.getRoot())) {
            if (curPath.resolve("modules").toFile().exists()) {
                Path targetPath = curPath.resolve("target");

                if (!targetPath.toFile().exists()) {
                    try {
                        Files.createDirectory(targetPath);
                    }
                    catch (IOException e) {
                        throw new RuntimeException("Failed to create target directory.", e);
                    }
                }

                return curPath.resolve("target").resolve("orphaned_tests.txt");
            }

            curPath = curPath.getParent();
        }

        throw new IllegalStateException("Can't find repository root directory.");
    }
}
