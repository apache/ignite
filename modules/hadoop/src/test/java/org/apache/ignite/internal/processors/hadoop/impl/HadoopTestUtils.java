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

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for tests.
 */
public class HadoopTestUtils {
    /** Base test directory. */
    private static final File BASE_TEST_DIR = new File(U.getIgniteHome() + "/work/test/hadoop/");

    /**
     * @return Base directory for tests.
     */
    public static File baseTestDir() {
        return BASE_TEST_DIR;
    }

    /**
     * Get test directory.
     *
     * @param parts Parts.
     * @return Directory.
     */
    public static File testDir(String... parts) {
        File res = BASE_TEST_DIR;

        if (parts != null) {
            for (String part : parts)
                res = new File(res, part);
        }

        return res;
    }

    /**
     * Clear base test directory.
     */
    public static void clearBaseTestDir() {
        if (baseTestDir().exists())
            assert delete(baseTestDir());
    }

    /**
     * Checks that job statistics file contains valid strings only.
     *
     * @param reader Buffered reader to get lines of job statistics.
     * @return Amount of events.
     * @throws IOException If failed.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static long simpleCheckJobStatFile(BufferedReader reader) throws IOException {
        Collection<String> phases = new HashSet<>();

        phases.add("submit");
        phases.add("prepare");
        phases.add("start");
        phases.add("finish");
        phases.add("requestId");
        phases.add("responseId");

        Collection<String> evtTypes = new HashSet<>();

        evtTypes.add("JOB");
        evtTypes.add("SETUP");
        evtTypes.add("MAP");
        evtTypes.add("SHUFFLE");
        evtTypes.add("REDUCE");
        evtTypes.add("COMBINE");
        evtTypes.add("COMMIT");

        long evtCnt = 0;
        String line;

        Map<Long, String> reduceNodes = new HashMap<>();

        while((line = reader.readLine()) != null) {
            String[] splitLine = line.split(":");

            //Try parse timestamp
            Long.parseLong(splitLine[1]);

            String[] evt = splitLine[0].split(" ");

            assertTrue("Unknown event '" + evt[0] + "'", evtTypes.contains(evt[0]));

            String phase;

            if ("JOB".equals(evt[0]))
                phase = evt[1];
            else {
                assertEquals(4, evt.length);
                assertTrue("The node id is not defined", !F.isEmpty(evt[3]));

                long taskNum = Long.parseLong(evt[1]);

                if (("REDUCE".equals(evt[0]) || "SHUFFLE".equals(evt[0]))) {
                    String nodeId = reduceNodes.get(taskNum);

                    if (nodeId == null)
                        reduceNodes.put(taskNum, evt[3]);
                    else
                        assertEquals("Different nodes for SHUFFLE and REDUCE tasks", nodeId, evt[3]);
                }

                phase = evt[2];
            }

            assertTrue("Unknown phase '" + phase + "' in " + Arrays.toString(evt), phases.contains(phase));

            evtCnt++;
        }

        return evtCnt;
    }

    /**
     * Deletes file or directory with all sub-directories and files.
     *
     * @param file File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(@Nullable File file) {
        if (file == null)
            return false;

        boolean res = true;

        if (file.isDirectory()) {
            File[] files = file.listFiles();

            if (files != null && files.length > 0)
                for (File file1 : files)
                    if (file1.isDirectory())
                        res &= delete(file1);
                    else
                        res &= file1.delete();

            res &= file.delete();
        }
        else
            res = file.delete();

        return res;
    }
}
