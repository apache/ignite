/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test for {@link GridNioSessionMetaKey}.
 */
public class GridNioSessionMetaKeySelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testNextRandomKey() throws Exception {
        AtomicInteger keyGen = U.staticField(GridNioSessionMetaKey.class, "keyGen");

        int initVal = keyGen.get();

        int key = GridNioSessionMetaKey.nextUniqueKey();

        // Check key is greater than any real GridNioSessionMetaKey ordinal.
        assertTrue(key >= GridNioSessionMetaKey.values().length);

        // Check all valid and some invalid key values.
        for (int i = ++key; i < GridNioSessionMetaKey.MAX_KEYS_CNT + 10; i++) {
            if (i < GridNioSessionMetaKey.MAX_KEYS_CNT)
                assertEquals(i, GridNioSessionMetaKey.nextUniqueKey());
            else
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return GridNioSessionMetaKey.nextUniqueKey();
                    }
                }, IllegalStateException.class, "Maximum count of NIO session keys in system is limited by");
        }

        keyGen.set(initVal);
    }
}
