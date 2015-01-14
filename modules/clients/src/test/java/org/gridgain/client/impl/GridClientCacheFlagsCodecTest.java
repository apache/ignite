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

package org.gridgain.client.impl;

import junit.framework.*;
import org.gridgain.client.*;
import org.gridgain.client.impl.connection.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.client.GridClientCacheFlag.*;

/**
 * Tests conversions between GridClientCacheFlag and GridCacheFlag.
 */
public class GridClientCacheFlagsCodecTest extends TestCase {
    /**
     * Tests that each client flag will be correctly converted to server flag.
     */
    public void testEncodingDecodingFullness() {
        for (GridClientCacheFlag f : GridClientCacheFlag.values()) {
            if (f == KEEP_PORTABLES)
                continue;

            int bits = GridClientConnection.encodeCacheFlags(Collections.singleton(f));

            assertTrue(bits != 0);

            GridCacheFlag[] out = GridCacheCommandHandler.parseCacheFlags(bits);
            assertEquals(1, out.length);

            assertEquals(f.name(), out[0].name());
        }
    }

    /**
     * Tests that groups of client flags can be correctly converted to corresponding server flag groups.
     */
    public void testGroupEncodingDecoding() {
        // all
        doTestGroup(GridClientCacheFlag.values());
        // none
        doTestGroup();
        // some
        doTestGroup(GridClientCacheFlag.INVALIDATE);
    }

    /**
     * @param flags Client flags to be encoded, decoded and checked.
     */
    private void doTestGroup(GridClientCacheFlag... flags) {
        EnumSet<GridClientCacheFlag> flagSet = F.isEmpty(flags) ? EnumSet.noneOf(GridClientCacheFlag.class) :
            EnumSet.copyOf(Arrays.asList(flags));

        int bits = GridClientConnection.encodeCacheFlags(flagSet);

        GridCacheFlag[] out = GridCacheCommandHandler.parseCacheFlags(bits);

        assertEquals(flagSet.contains(KEEP_PORTABLES) ? flagSet.size() - 1 : flagSet.size(), out.length);

        for (GridCacheFlag f : out) {
            assertTrue(flagSet.contains(GridClientCacheFlag.valueOf(f.name())));
        }
    }
}
