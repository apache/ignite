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

package org.apache.ignite.examples;

import org.apache.ignite.examples.ignitefs.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * IgniteFs examples self test.
 */
public class IgfsExamplesSelfTest extends GridAbstractExamplesTest {
    /** IgniteFs config with shared memory IPC. */
    private static final String IGNITEFS_SHMEM_CFG = "modules/core/src/test/config/ggfs-shmem.xml";

    /** IgniteFs config with loopback IPC. */
    private static final String IGNITEFS_LOOPBACK_CFG = "modules/core/src/test/config/ggfs-loopback.xml";

    /**
     * @throws Exception If failed.
     */
    public void testIgniteFsApiExample() throws Exception {
        String configPath = U.isWindows() ? IGNITEFS_LOOPBACK_CFG : IGNITEFS_SHMEM_CFG;

        try {
            startGrid("test1", configPath);
            startGrid("test2", configPath);
            startGrid("test3", configPath);

            IgfsExample.main(EMPTY_ARGS);
        }
        finally {
            stopAllGrids();
        }
    }
}
