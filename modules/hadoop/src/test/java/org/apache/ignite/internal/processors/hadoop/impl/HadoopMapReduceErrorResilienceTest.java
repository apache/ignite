/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test of error resiliency after an error in a map-reduce job execution.
 * Combinations tested:
 * { new ALI, old API }
 *   x { unchecked exception, checked exception, error }
 *   x { phase where the error happens }.
 */
@RunWith(JUnit4.class)
public class HadoopMapReduceErrorResilienceTest extends HadoopAbstractMapReduceTest {
    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryAfterAnError0_Runtime() throws Exception {
        doTestRecoveryAfterAnError(0, HadoopErrorSimulator.Kind.Runtime);
    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryAfterAnError0_IOException() throws Exception {
        doTestRecoveryAfterAnError(0, HadoopErrorSimulator.Kind.IOException);
    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryAfterAnError0_Error() throws Exception {
        doTestRecoveryAfterAnError(0, HadoopErrorSimulator.Kind.Error);
    }

    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryAfterAnError7_Runtime() throws Exception {
        doTestRecoveryAfterAnError(7, HadoopErrorSimulator.Kind.Runtime);
    }
    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryAfterAnError7_IOException() throws Exception {
        doTestRecoveryAfterAnError(7, HadoopErrorSimulator.Kind.IOException);
    }
    /**
     * Tests recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecoveryAfterAnError7_Error() throws Exception {
        doTestRecoveryAfterAnError(7, HadoopErrorSimulator.Kind.Error);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /**
     * Tests correct work after an error.
     *
     * @throws Exception On error.
     */
    private void doTestRecoveryAfterAnError(int useNewBits, HadoopErrorSimulator.Kind simulatorKind) throws Exception {
        try {
            IgfsPath inDir = new IgfsPath(PATH_INPUT);

            igfs.mkdirs(inDir);

            IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

            generateTestFile(inFile.toString(), "red", red, "blue", blue, "green", green, "yellow", yellow);

            boolean useNewMapper = (useNewBits & 1) == 0;
            boolean useNewCombiner = (useNewBits & 2) == 0;
            boolean useNewReducer = (useNewBits & 4) == 0;

            for (int i = 0; i < 12; i++) {
                int bits = 1 << i;

                System.out.println("############################ Simulator kind = " + simulatorKind
                    + ", Stage bits = " + bits);

                HadoopErrorSimulator sim = HadoopErrorSimulator.create(simulatorKind, bits);

                doTestWithErrorSimulator(sim, inFile, useNewMapper, useNewCombiner, useNewReducer);
            }
        } catch (Throwable t) {
            t.printStackTrace();

            fail("Unexpected throwable: " + t);
        }
    }

    /**
     * Performs test with given error simulator.
     *
     * @param sim The simulator.
     * @param inFile Input file.
     * @param useNewMapper If the use new mapper API.
     * @param useNewCombiner If to use new combiner.
     * @param useNewReducer If to use new reducer API.
     * @throws Exception If failed.
     */
    private void doTestWithErrorSimulator(HadoopErrorSimulator sim, IgfsPath inFile, boolean useNewMapper,
        boolean useNewCombiner, boolean useNewReducer) throws Exception {
        // Set real simulating error simulator:
        assertTrue(HadoopErrorSimulator.setInstance(HadoopErrorSimulator.noopInstance, sim));

        try {
            // Expect failure there:
            doTest(inFile, useNewMapper, useNewCombiner, useNewReducer);
        }
        catch (Throwable t) { // This may be an Error.
            // Expected:
            System.out.println(t.toString()); // Ignore, continue the test.
        }

        // Set no-op error simulator:
        assertTrue(HadoopErrorSimulator.setInstance(sim, HadoopErrorSimulator.noopInstance));

        // Expect success there:
        doTest(inFile, useNewMapper, useNewCombiner, useNewReducer);
    }
}
