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

package org.apache.ignite.examples;

import org.apache.ignite.examples.computegrid.ComputeBroadcastExample;
import org.apache.ignite.examples.computegrid.ComputeCallableExample;
import org.apache.ignite.examples.computegrid.ComputeClosureExample;
import org.apache.ignite.examples.computegrid.ComputeRunnableExample;
import org.apache.ignite.examples.datastructures.IgniteExecutorServiceExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 * Closure examples self test.
 */
public class BasicExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBroadcastExample() throws Exception {
        ComputeBroadcastExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCallableExample() throws Exception {
        ComputeCallableExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClosureExample() throws Exception {
        ComputeClosureExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecutorExample() throws Exception {
        IgniteExecutorServiceExample.main(EMPTY_ARGS);
    }

//    TODO: IGNITE-711 next example(s) should be implemented for java 8 or testing method(s) should be removed if example(s) does not applicable for java 8.
//    /**
//     * @throws Exception If failed.
//     */
//    public void testReducerExample() throws Exception {
//        ComputeReducerExample.main(EMPTY_ARGS);
//    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRunnableExample() throws Exception {
        ComputeRunnableExample.main(EMPTY_ARGS);
    }

//    TODO: IGNITE-711 next example(s) should be implemented for java 8
//    or testing method(s) should be removed if example(s) does not applicable for java 8.
//    /**
//     * @throws Exception If failed.
//     */
//    public void testTaskMapExample() throws Exception {
//        ComputeTaskMapExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testTaskSplitExample() throws Exception {
//        ComputeTaskSplitExample.main(EMPTY_ARGS);
//    }
}
