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

import org.junit.Ignore;
import org.junit.Test;

/**
 * Same test as HadoopMapReduceTest, but with enabled Snappy output compression.
 */
public class HadoopSnappyFullMapReduceTest extends HadoopMapReduceTest {
    /** {@inheritDoc} */
    @Override protected boolean compressOutputSnappy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean[][] getApiModes() {
        return new boolean[][] {
            { false, false, true },
            { true, true, true },
        };
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9920")
    @Test
    @Override public void testWholeMapReduceExecution() throws Exception {
        super.testWholeMapReduceExecution();
    }
}
