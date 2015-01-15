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

package org.gridgain.examples;

import org.apache.ignite.configuration.*;
import org.gridgain.examples.ggfs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

/**
 * GGFS examples self test.
 */
public class GridGgfsExamplesSelfTest extends GridAbstractExamplesTest {
    /** Grid name for light client example. */
    private static final String CLIENT_LIGHT_GRID_NAME = "client-light-grid";

    /** GGFS config with shared memory IPC. */
    private static final String GGFS_SHMEM_CFG = "modules/core/src/test/config/ggfs-shmem.xml";

    /** GGFS config with loopback IPC. */
    private static final String GGFS_LOOPBACK_CFG = "modules/core/src/test/config/ggfs-loopback.xml";

    /** GGFS no endpoint config. */
    private static final String GGFS_NO_ENDPOINT_CFG = "modules/core/src/test/config/ggfs-no-endpoint.xml";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        String cfgPath = gridName == null ? (U.isWindows() ? GGFS_LOOPBACK_CFG : GGFS_SHMEM_CFG) :
            GGFS_NO_ENDPOINT_CFG;

        IgniteConfiguration cfg = GridGainEx.loadConfiguration(cfgPath).get1();

        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGgfsApiExample() throws Exception {
        startGrids(3);

        try {
            GgfsExample.main(EMPTY_ARGS);
        }
        finally {
            stopAllGrids();
        }
    }
}
