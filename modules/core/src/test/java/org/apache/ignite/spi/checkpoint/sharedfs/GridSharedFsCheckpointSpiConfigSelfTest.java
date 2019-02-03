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

package org.apache.ignite.spi.checkpoint.sharedfs;

import java.util.LinkedList;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Grid shared file system checkpoint SPI config self test.
 */
@GridSpiTest(spi = SharedFsCheckpointSpi.class, group = "Checkpoint SPI")
public class GridSharedFsCheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<SharedFsCheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new SharedFsCheckpointSpi(), "directoryPaths", null);
        checkNegativeSpiProperty(new SharedFsCheckpointSpi(), "directoryPaths", new LinkedList<String>());
    }
}
