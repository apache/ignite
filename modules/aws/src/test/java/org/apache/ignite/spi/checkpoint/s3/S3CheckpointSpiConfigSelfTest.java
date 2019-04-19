/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.checkpoint.s3;

import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Grid S3 checkpoint SPI config self test.
 */
@GridSpiTest(spi = S3CheckpointSpi.class, group = "Checkpoint SPI")
public class S3CheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<S3CheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new S3CheckpointSpi(), "awsCredentials", null);
    }
}
