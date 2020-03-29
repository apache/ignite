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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsDataRegionMetricsTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsDataRegionMetricsTxTest extends IgnitePdsDataRegionMetricsTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMvccVacuumFrequency(Long.MAX_VALUE);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Object, Object> cacheConfiguration() {
        return super.cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10662")
    @Test
    @Override public void testMemoryUsageMultipleNodes() throws Exception {
        super.testMemoryUsageMultipleNodes();
    }
}
