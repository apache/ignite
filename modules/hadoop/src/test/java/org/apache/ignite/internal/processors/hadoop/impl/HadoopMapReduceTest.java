/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount2;
import org.junit.Test;

/**
 * Test of whole cycle of map-reduce processing via Job tracker.
 */
public class HadoopMapReduceTest extends HadoopAbstractMapReduceTest {
    /**
     * Tests whole job execution with all phases in all combination of new and old versions of API.
     * @throws Exception If fails.
     */
    @Test
    public void testWholeMapReduceExecution() throws Exception {
        IgfsPath inDir = new IgfsPath(PATH_INPUT);

        igfs.mkdirs(inDir);

        IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

        generateTestFile(inFile.toString(), "red", red, "blue", blue, "green", green, "yellow", yellow );

        for (boolean[] apiMode: getApiModes()) {
            assert apiMode.length == 3;

            boolean useNewMapper = apiMode[0];
            boolean useNewCombiner = apiMode[1];
            boolean useNewReducer = apiMode[2];

            doTest(inFile, useNewMapper, useNewCombiner, useNewReducer);
        }
    }

    /**
     * Gets API mode combinations to be tested.
     * Each boolean[] is { newMapper, newCombiner, newReducer } flag triplet.
     *
     * @return Arrays of booleans indicating API combinations to test.
     */
    protected boolean[][] getApiModes() {
        return new boolean[][] {
            { false, false, false },
            { false, false, true },
            { false, true,  false },
            { true,  false, false },
            { true,  true,  true },
        };
    }
}
