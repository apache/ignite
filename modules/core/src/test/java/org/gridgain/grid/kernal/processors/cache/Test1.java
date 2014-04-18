// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.util.offheap.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;


public class Test1 extends GridCommonAbstractTest {
    public void test() throws Exception {
        GridUnsafeMap0 map = new GridUnsafeMap0((short)512);

        GridOptimizedMarshaller marsh = new GridOptimizedMarshaller();

        Random rnd = new Random();

        byte[] val = new byte[160];

        for (int i = 0; i < 160; i++)
            val[i] = (byte)(114 + i);

        for (;;) {
            Long key = rnd.nextLong();
            byte[] keyBytes = marsh.marshal(key);

            map.put(keyBytes, val);
        }
    }
}
