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

import org.apache.ignite.examples.compute.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Closure examples self test.
 */
public class GridBasicExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridBroadcastExample() throws Exception {
        ComputeBroadcastExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCallableExample() throws Exception {
        ComputeCallableExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridClosureExample() throws Exception {
        ComputeClosureExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridExecutorExample() throws Exception {
        ComputeExecutorServiceExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridReducerExample() throws Exception {
        ComputeReducerExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridRunnableExample() throws Exception {
        ComputeRunnableExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridTaskMapExample() throws Exception {
        ComputeTaskMapExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridTaskSplitExample() throws Exception {
        ComputeTaskSplitExample.main(EMPTY_ARGS);
    }
}
