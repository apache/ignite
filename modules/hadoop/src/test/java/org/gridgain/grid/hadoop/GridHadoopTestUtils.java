/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Utility class for tests.
 */
public class GridHadoopTestUtils {
    /**
     * Checks that job statistics file contains valid strings only.
     *
     * @param reader Buffered reader to get lines of job statistics.
     * @return Amount of events.
     * @throws IOException If failed.
     */
    public static long simpleCheckJobStatFile(BufferedReader reader) throws IOException {
        Collection<String> phases = new HashSet<>();

        phases.add("submit");
        phases.add("prepare");
        phases.add("start");
        phases.add("run");
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
                //Try parse task number
                Long.parseLong(evt[1]);
                phase = evt[2];
            }

            assertTrue("Unknown phase '" + phase + "' in " + Arrays.toString(evt), phases.contains(phase));

            evtCnt++;
        }

        return evtCnt;
    }
}
