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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test {@link ZkIgnitePaths#validatePath} implementation.
 */
@RunWith(Enclosed.class)
public class ZookeeperValidatePathsTest {
    /**
     *  Parameterized test for testing common cases, excluding unprintable characters.
     */
    @RunWith(Parameterized.class)
    public static class ZoookeperCommonValidatePathsTest extends GridCommonAbstractTest {
        /** */
        @Parameterized.Parameters(name = "input string = {0}, expected error = {1}")
        public static Collection<Object[]> parameters() {
            return Arrays.asList(
                new Object[] {"/apacheIgnite", null},
                new Object[] {null, "Path cannot be null"},
                new Object[] {"", "Path length must be > 0"},
                new Object[] {"/apacheIgnite/../root", "relative paths not allowed @15"},
                new Object[] {"/apacheIgnite/./root", "relative paths not allowed @14"},
                new Object[] {"/apacheIgnite//root", "empty node name specified @14"},
                new Object[] {"/apacheIgnite/", "Path must not end with / character"}
            );
        }

        /** Zookeeper path to validate. */
        @Parameterized.Parameter
        public String zkPath;

        /** Expected error. If {@code null}, path is correct. */
        @Parameterized.Parameter(1)
        public String errMsg;

        /** */
        @Test
        public void testValidatePath() {
            validatePath(zkPath, errMsg);
        }
    }

    /**
     * Test validate path with unprintable characters. Should move it to separate test because of
     * surefire report problems on TC.
     */
    public static class ZookeeperUnprintableCharactersValidatePathTest extends GridCommonAbstractTest {
        /** */
        @Test
        public void testValidatePathWithUnprintableCharacters() {
            for (Character c: unprintables())
                validatePath(String.format("/apacheIgnite%s", c), "invalid charater @13");

            validatePath(String.format("/apacheIgnite%s", '\u0000'), "null character not allowed @13");
        }
    }

    /** */
    private static void validatePath(String path, String msg) {
        try {
            ZkIgnitePaths.validatePath(path);

            if (msg != null)
                Assert.fail(String.format("Expected failure containing \"%s\" in message did't occur", msg));
        }
        catch (IllegalArgumentException e) {
            if (msg == null) {
                Assert.fail(String.format("Path \"%s\" should be considering as valid, but failing with \"%s\"",
                    path, e.getMessage()));
            }
            else {
                Assert.assertTrue(String.format("Error messages \"%s\" does't contains \"%s\"", e.getMessage(), msg),
                    e.getMessage().contains(msg));
            }
        }
    }

    /**
     * @return List of unprintables characters.
     */
    private static List<Character> unprintables() {
        List<Character> ret = new ArrayList<>();

        List<IgnitePair<Character>> intervals = Arrays.asList(
            new IgnitePair<>('\u0000', '\u001f'),
            new IgnitePair<>('\u007f', '\u009f'),
            new IgnitePair<>('\ud800', '\uf8ff'),
            new IgnitePair<>('\ufff0', '\uffff')
        );

        for (IgnitePair<Character> interval: intervals) {
            for (char c = (char)(interval.get1() + 1); c < interval.get2(); c++)
                ret.add(c);
        }

        return ret;
    }
}
