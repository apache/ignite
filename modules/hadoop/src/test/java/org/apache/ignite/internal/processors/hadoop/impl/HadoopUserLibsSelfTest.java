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

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.internal.processors.hadoop.HadoopClasspathUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

/**
 * Tests for user libs parsing.
 */
public class HadoopUserLibsSelfTest extends GridCommonAbstractTest {
    /** Directory 1. */
    private static final File DIR_1 = HadoopTestUtils.testDir("dir1");

    /** File 1 in directory 1. */
    private static final File FILE_1_1 = new File(DIR_1, "file1.jar");

    /** File 2 in directory 1. */
    private static final File FILE_1_2 = new File(DIR_1, "file2.jar");

    /** Directory 2. */
    private static final File DIR_2 = HadoopTestUtils.testDir("dir2");

    /** File 1 in directory 2. */
    private static final File FILE_2_1 = new File(DIR_2, "file1.jar");

    /** File 2 in directory 2. */
    private static final File FILE_2_2 = new File(DIR_2, "file2.jar");

    /** Missing directory. */
    private static final File MISSING_DIR = HadoopTestUtils.testDir("missing_dir");

    /** Missing file. */
    private static final File MISSING_FILE = new File(MISSING_DIR, "file.jar");

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        HadoopTestUtils.clearBaseTestDir();

        assert DIR_1.mkdirs();
        assert DIR_2.mkdirs();

        assert FILE_1_1.createNewFile();
        assert FILE_1_2.createNewFile();
        assert FILE_2_1.createNewFile();
        assert FILE_2_2.createNewFile();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // Sanity checks before test start.
        ensureExists(FILE_1_1);
        ensureExists(FILE_1_2);
        ensureExists(FILE_2_1);
        ensureExists(FILE_2_2);

        ensureNotExists(MISSING_DIR);
        ensureNotExists(MISSING_FILE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        HadoopTestUtils.clearBaseTestDir();
    }

    /**
     * Test null or empty user libs.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNullOrEmptyUserLibs() throws Exception {
        assert parse(null).isEmpty();
        assert parse("").isEmpty();
    }

    /**
     * Test single file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingle() throws Exception {
        Collection<File> res = parse(single(FILE_1_1));

        assert res.size() == 1;
        assert res.contains(FILE_1_1);

        res = parse(single(MISSING_FILE));

        assert res.size() == 0;
    }

    /**
     * Test multiple files.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiple() throws Exception {
        Collection<File> res =
            parse(merge(single(FILE_1_1), single(FILE_1_2), single(FILE_2_1), single(FILE_2_2), single(MISSING_FILE)));

        assert res.size() == 4;
        assert res.contains(FILE_1_1);
        assert res.contains(FILE_1_2);
        assert res.contains(FILE_2_1);
        assert res.contains(FILE_2_2);
    }

    /**
     * Test single wildcard.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleWildcard() throws Exception {
        Collection<File> res = parse(wildcard(DIR_1));

        assert res.size() == 2;
        assert res.contains(FILE_1_1);
        assert res.contains(FILE_1_2);

        res = parse(wildcard(MISSING_DIR));

        assert res.size() == 0;
    }

    /**
     * Test multiple wildcards.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleWildcards() throws Exception {
        Collection<File> res = parse(merge(wildcard(DIR_1), wildcard(DIR_2), wildcard(MISSING_DIR)));

        assert res.size() == 4;
        assert res.contains(FILE_1_1);
        assert res.contains(FILE_1_2);
        assert res.contains(FILE_2_1);
        assert res.contains(FILE_2_2);
    }

    /**
     * Test mixed tokens.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixed() throws Exception {
        String str = merge(
            single(FILE_1_1),
            wildcard(DIR_2),
            single(MISSING_FILE),
            wildcard(MISSING_DIR)
        );

        Collection<File> res = parse(str);

        assert res.size() == 3;
        assert res.contains(FILE_1_1);
        assert res.contains(FILE_2_1);
        assert res.contains(FILE_2_2);
    }

    /**
     * Ensure provided file exists.
     *
     * @param file File.
     */
    private static void ensureExists(File file) {
        assert file.exists();
    }

    /**
     * Ensure provided file doesn't exist.
     *
     * @param file File.
     */
    private static void ensureNotExists(File file) {
        assert !file.exists();
    }

    /**
     * Merge string using path separator.
     *
     * @param vals Values.
     * @return Result.
     */
    private static String merge(String... vals) {
        StringBuilder res = new StringBuilder();

        if (vals != null) {
            boolean first = true;

            for (String val : vals) {
                if (first)
                    first = false;
                else
                    res.append(File.pathSeparatorChar);

                res.append(val);
            }
        }

        return res.toString();
    }

    /**
     * Parse string.
     *
     * @param str String.
     * @return Files.
     * @throws IOException If failed.
     */
    Collection<File> parse(String str) throws IOException {
        Collection<HadoopClasspathUtils.SearchDirectory> dirs = HadoopClasspathUtils.parseUserLibs(str);

        Collection<File> res = new HashSet<>();

        for (HadoopClasspathUtils.SearchDirectory dir : dirs)
            Collections.addAll(res, dir.files());

        return res;
    }

    /**
     * Get absolute path to a single file.
     *
     * @param file File.
     * @return Path.
     */
    private static String single(File file) {
        return file.getAbsolutePath();
    }

    /**
     * Create a wildcard.
     *
     * @param file File.
     * @return Wildcard.
     */
    private static String wildcard(File file) {
        return file.getAbsolutePath() + File.separatorChar + "*";
    }
}
