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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 *
 * TODO IGNITE-2310: use JobStealingCollisionSpi.
 */
public class IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest extends IgniteCacheLockPartitionOnAffinityRunTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        JobStealingCollisionSpi colSpi = new JobStealingCollisionSpi();
        // One job at a time.
        colSpi.setActiveJobsThreshold(1);
        colSpi.setWaitJobsThreshold(1);
        colSpi.setMessageExpireTime(10_000);
        colSpi.setMaximumStealingAttempts(1);

        cfg.setCollisionSpi(colSpi);

        return cfg;
    }
}