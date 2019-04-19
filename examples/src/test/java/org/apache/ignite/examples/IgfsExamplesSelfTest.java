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

package org.apache.ignite.examples;

//import org.apache.ignite.examples.igfs.*;
//import org.apache.ignite.internal.util.typedef.internal.*;

import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Ignore;

/**
 * IGFS examples self test.
 */
@Ignore("https://issues.apache.org/jira/browse/IGNITE-711")
public class IgfsExamplesSelfTest extends GridAbstractExamplesTest {
    /** IGFS config with shared memory IPC. */
    private static final String IGFS_SHMEM_CFG = "modules/core/src/test/config/igfs-shmem.xml";

    /** IGFS config with loopback IPC. */
    private static final String IGFS_LOOPBACK_CFG = "modules/core/src/test/config/igfs-loopback.xml";

    /**
     * TODO: IGNITE-711 next example(s) should be implemented for java 8
     * or testing method(s) should be removed if example(s) does not applicable for java 8.
     *
     * @throws Exception If failed.
     */
//    public void testIgniteFsApiExample() throws Exception {
//        String configPath = U.isWindows() ? IGFS_LOOPBACK_CFG : IGFS_SHMEM_CFG;
//
//        try {
//            startGrid("test1", configPath);
//            startGrid("test2", configPath);
//            startGrid("test3", configPath);
//
//            IgfsExample.main(EMPTY_ARGS);
//        }
//        finally {
//            stopAllGrids();
//        }
//    }
}
