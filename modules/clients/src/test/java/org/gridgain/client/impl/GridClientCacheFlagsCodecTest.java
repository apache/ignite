/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
