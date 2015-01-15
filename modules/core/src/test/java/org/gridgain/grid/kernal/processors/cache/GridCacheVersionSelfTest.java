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

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 *
 */
public class GridCacheVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testTopologyVersionDrId() throws Exception {
        GridCacheVersion ver = version(10, 0);

        assertEquals(10, ver.nodeOrder());
        assertEquals(0, ver.dataCenterId());

        // Check with max topology version and some dr IDs.
        ver = version(0x7FFFFFF, 0);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(0, ver.dataCenterId());

        ver = version(0x7FFFFFF, 15);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(15, ver.dataCenterId());

        ver = version(0x7FFFFFF, 31);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        // Check max dr ID with some topology versions.
        ver = version(11, 31);
        assertEquals(11, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        ver = version(256, 31);
        assertEquals(256, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        ver = version(1025, 31);
        assertEquals(1025, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        // Check overflow exception.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return version(0x7FFFFFF + 1, 1);
            }
        }, IllegalArgumentException.class, null);
    }

    /**
     * @param nodeOrder Node order.
     * @param drId Data center ID.
     * @return Cache version.
     */
    private GridCacheVersion version(int nodeOrder, int drId) {
        return new GridCacheVersion(0, 0, 0, nodeOrder, drId);
    }
}
