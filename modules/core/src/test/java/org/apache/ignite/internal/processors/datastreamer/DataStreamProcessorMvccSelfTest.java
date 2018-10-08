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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

/**
 * Check DataStreamer with Mvcc enabled.
 */
public class DataStreamProcessorMvccSelfTest extends DataStreamProcessorSelfTest {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteConfiguration = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = igniteConfiguration.getCacheConfiguration();

        if (ccfgs != null) {
            for (CacheConfiguration ccfg : ccfgs)
                ccfg.setNearConfiguration(null);
        }

        assert ccfgs == null || ccfgs.length == 0 ||
            (ccfgs.length == 1 && ccfgs[0].getAtomicityMode() == TRANSACTIONAL_SNAPSHOT);

        return igniteConfiguration;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode getCacheAtomicityMode() {
        return TRANSACTIONAL_SNAPSHOT;
    }

    /** {@inheritDoc} */
    @Override public void testUpdateStore() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8582");

        super.testUpdateStore();
    }

    /** {@inheritDoc} */
    @Override public void testFlushTimeout() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9321");
    }

    /** {@inheritDoc} */
    @Override public void testLocal() {
        // Do not check local caches with MVCC enabled.
    }
}
