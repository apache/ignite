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
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTest;
import org.junit.Test;

/**
 * Mvcc CQ client test.
 */
public class CacheMvccContinuousQueryClientTest extends IgniteCacheContinuousQueryClientTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 90 * 60 * 1000; // 90 minutes
    }

    /** {@inheritDoc} */

    @Test
    @Override public void testNodeJoinsRestartQuery() throws Exception {
        long start = System.currentTimeMillis();
        long end = (long)(start + getTestTimeout() * 0.9);

        long iteration = 0;

        while (System.currentTimeMillis() < end) {
            System.out.println("========iteration=" + iteration + ", left " + (end - System.currentTimeMillis()) / 1000 + " sec ");

            super.beforeTest();

            super.testNodeJoinsRestartQuery();

            super.afterTest();

            iteration++;
        }
    }
}
