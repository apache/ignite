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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Checks that user-defined code for data streamer is executed inside the sandbox.
 */
public class DataStreamerSandboxTest extends AbstractSandboxTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(TEST_CACHE));
    }

    /** */
    @Test
    public void test() throws Exception {
        runOperation(operation(grid(CLNT_ALLOWED_WRITE_PROP)));
        runForbiddenOperation(operation(grid(CLNT_FORBIDDEN_WRITE_PROP)), AccessControlException.class);
    }

    /**
     * @return Operation to test.
     */
    private GridTestUtils.RunnableX operation(Ignite node) {
        return () -> {
            try (IgniteDataStreamer<Integer, Integer> strm = node.dataStreamer(TEST_CACHE)) {
                strm.receiver((cache, entries) -> controlAction());

                strm.addData(1, 100);
            }
        };
    }
}
