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
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingRebalanceErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        //GridDhtPartitionSupplyMessage unmarshalling failed test.
        readCnt.set(Integer.MAX_VALUE);

        for (int i = 0; i <= 20; i++)
            jcache(0).put(new TestKey(String.valueOf(++key)), "");

        readCnt.set(1);

        startGrid(3);

        // GridDhtPartitionSupplyMessage unmarshalling failed but ioManager does not hangs up.

        Thread.sleep(1000);

        // GridDhtForceKeysRequest unmarshalling failed test.
        stopGrid(3);

        readCnt.set(Integer.MAX_VALUE);

        for (int i = 0; i <= 100; i++)
            jcache(0).put(new TestKey(String.valueOf(++key)), "");

        startGrid(10); // Custom rebalanceDelay set at cfg.

        Affinity<Object> aff = affinity(grid(10).cache(DEFAULT_CACHE_NAME));

        GridCacheContext cctx = grid(10).context().cache().cache(DEFAULT_CACHE_NAME).context();

        List<List<ClusterNode>> affAssign =
            cctx.affinity().assignment(cctx.affinity().affinityTopologyVersion()).idealAssignment();

        Integer part = null;

        ClusterNode node = grid(10).localNode();

        for (int p = 0; p < aff.partitions(); p++) {
            if (affAssign.get(p).get(0).equals(node)) {
                part = p;

                break;
            }
        }

        assertNotNull(part);

        long stopTime = U.currentTimeMillis() + 5000;

        while (!part.equals(aff.partition(new TestKey(String.valueOf(key))))) {
            --key;

            if (U.currentTimeMillis() > stopTime)
                fail();
        }

        readCnt.set(1);

        try {
            jcache(10).get(new TestKey(String.valueOf(key)));

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }
    }
}
