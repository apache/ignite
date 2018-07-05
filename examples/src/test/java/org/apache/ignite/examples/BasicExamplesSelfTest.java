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

package org.apache.ignite.examples;

import org.apache.ignite.examples.computegrid.ComputeBroadcastExample;
import org.apache.ignite.examples.computegrid.ComputeCallableExample;
import org.apache.ignite.examples.computegrid.ComputeClosureExample;
import org.apache.ignite.examples.computegrid.ComputeRunnableExample;
import org.apache.ignite.examples.datastructures.IgniteExecutorServiceExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 * Closure examples self test.
 */
public class BasicExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testBroadcastExample() throws Exception {
        ComputeBroadcastExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallableExample() throws Exception {
        ComputeCallableExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureExample() throws Exception {
        ComputeClosureExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
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
