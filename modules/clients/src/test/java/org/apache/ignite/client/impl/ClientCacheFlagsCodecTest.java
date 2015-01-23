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

package org.apache.ignite.client.impl;

import junit.framework.*;
import org.apache.ignite.client.*;
import org.apache.ignite.client.impl.connection.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.rest.handlers.cache.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;

import static org.apache.ignite.client.GridClientCacheFlag.*;

/**
 * Tests conversions between GridClientCacheFlag and CacheFlag.
 */
public class ClientCacheFlagsCodecTest extends TestCase {
    /**
     * Tests that each client flag will be correctly converted to server flag.
     */
    public void testEncodingDecodingFullness() {
        for (GridClientCacheFlag f : GridClientCacheFlag.values()) {
            if (f == KEEP_PORTABLES)
                continue;

            int bits = GridClientConnection.encodeCacheFlags(Collections.singleton(f));

            assertTrue(bits != 0);

            CacheFlag[] out = GridCacheCommandHandler.parseCacheFlags(bits);
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

        CacheFlag[] out = GridCacheCommandHandler.parseCacheFlags(bits);

        assertEquals(flagSet.contains(KEEP_PORTABLES) ? flagSet.size() - 1 : flagSet.size(), out.length);

        for (CacheFlag f : out) {
            assertTrue(flagSet.contains(GridClientCacheFlag.valueOf(f.name())));
        }
    }
}
