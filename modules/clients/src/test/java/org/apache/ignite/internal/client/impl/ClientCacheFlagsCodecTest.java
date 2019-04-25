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

package org.apache.ignite.internal.client.impl;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.apache.ignite.internal.client.GridClientCacheFlag;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests conversions between GridClientCacheFlag.
 */
public class ClientCacheFlagsCodecTest {
    /**
     * Tests that each client flag will be correctly converted to server flag.
     */
    @Test
    public void testEncodingDecodingFullness() {
        for (GridClientCacheFlag f : GridClientCacheFlag.values()) {
            int bits = GridClientCacheFlag.encodeCacheFlags(EnumSet.of(f));

            assertTrue(bits != 0);

            Set<GridClientCacheFlag> out = GridClientCacheFlag.parseCacheFlags(bits);

            assertTrue(out.contains(f));
        }
    }

    /**
     * Tests that groups of client flags can be correctly converted to corresponding server flag groups.
     */
    @Test
    public void testGroupEncodingDecoding() {
        // All.
        doTestGroup(GridClientCacheFlag.values());

        // None.
        doTestGroup();
    }

    /**
     * @param flags Client flags to be encoded, decoded and checked.
     */
    private void doTestGroup(GridClientCacheFlag... flags) {
        EnumSet<GridClientCacheFlag> flagSet = F.isEmpty(flags)
            ? EnumSet.noneOf(GridClientCacheFlag.class)
            : EnumSet.copyOf(Arrays.asList(flags));

        int bits = GridClientCacheFlag.encodeCacheFlags(flagSet);

        Set<GridClientCacheFlag> out = GridClientCacheFlag.parseCacheFlags(bits);

        assertTrue(out.containsAll(flagSet));
    }
}
