/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import javax.cache.CacheException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingRebalanceErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        //GridDhtPartitionSupplyMessage unmarshalling failed test.
        readCnt.set(Integer.MAX_VALUE);

        for (int i = 0; i <= 20; i++)
            jcache(0).put(new TestKey(String.valueOf(++key)), "");

        readCnt.set(1);

        startGrid(3);

        //GridDhtPartitionSupplyMessage unmarshalling failed but ioManager does not hangs up.

        Thread.sleep(1000);

        //GridDhtForceKeysRequest unmarshalling failed test.
        stopGrid(3);

        readCnt.set(Integer.MAX_VALUE);

        for (int i = 0; i <= 1000; i++)
            jcache(0).put(new TestKey(String.valueOf(++key)), "");

        startGrid(3);

        Affinity<Object> aff = affinity(grid(3).cache(null));

        while (!aff.isPrimary(grid(3).localNode(), new TestKey(String.valueOf(key))))
            --key;

        readCnt.set(1);

        try {
            jcache(3).get(new TestKey(String.valueOf(key)));

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }
    }
}