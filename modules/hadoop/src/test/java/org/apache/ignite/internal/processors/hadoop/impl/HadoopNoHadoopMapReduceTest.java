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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Test attempt to execute a map-reduce task while no Hadoop processor available.
 */
public class HadoopNoHadoopMapReduceTest extends HadoopMapReduceTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setHadoopConfiguration(null);
        c.setPeerClassLoadingEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testWholeMapReduceExecution() throws Exception {
        try {
            super.testWholeMapReduceExecution();

            fail("IllegalStateException expected.");
        }
        catch (IllegalStateException ignore) {
            // No-op.
        }
    }
}
