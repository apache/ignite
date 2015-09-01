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

package org.apache.ignite.spi.collision.jobstealing;

import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 * Job stealing collision SPI config test.
 */
@GridSpiTest(spi = JobStealingCollisionSpi.class, group = "Collision SPI")
public class GridJobStealingCollisionSpiConfigSelfTest extends GridSpiAbstractConfigTest<JobStealingCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new JobStealingCollisionSpi(), "messageExpireTime", 0);
        checkNegativeSpiProperty(new JobStealingCollisionSpi(), "waitJobsThreshold", -1);
        checkNegativeSpiProperty(new JobStealingCollisionSpi(), "activeJobsThreshold", -1);
        checkNegativeSpiProperty(new JobStealingCollisionSpi(), "maximumStealingAttempts", 0);
    }
}