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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.processors.cache.version.CacheVersion;
import org.apache.ignite.internal.processors.cache.version.CacheVersionImpl;
import org.apache.ignite.internal.processors.cache.version.CacheVersionImplEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testTopologyVersionDrId() throws Exception {
        testTopologyVersionDrId(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyVersionDrIdNewVer() throws Exception {
        testTopologyVersionDrId(true);
    }

    /**
     * @param newVer If {@code true} tests {@link CacheVersionImpl}.
     * @throws Exception If failed.
     */
    private void testTopologyVersionDrId(final boolean newVer) throws Exception {
        CacheVersion ver = version(10, 0, newVer);

        assertEquals(10, ver.nodeOrder());
        assertEquals(0, ver.dataCenterId());

        // Check with max topology version and some dr IDs.
        ver = version(0x7FFFFFF, 0, newVer);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(0, ver.dataCenterId());

        ver = version(0x7FFFFFF, 15, newVer);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(15, ver.dataCenterId());

        ver = version(0x7FFFFFF, 31, newVer);
        assertEquals(0x7FFFFFF, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        // Check max dr ID with some topology versions.
        ver = version(11, 31, newVer);
        assertEquals(11, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        ver = version(256, 31, newVer);
        assertEquals(256, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        ver = version(1025, 31, newVer);
        assertEquals(1025, ver.nodeOrder());
        assertEquals(31, ver.dataCenterId());

        // Check overflow exception.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return version(0x7FFFFFF + 1, 1, newVer);
            }
        }, IllegalArgumentException.class, null);
    }

    /**
     *
     */
    public void testMinorTopologyVersion() {
        int MAX_TOP_VER = 0x00_FF_FF_FF;
        int MAX_MINOR_VER = 0x00_00_FF_FF;
        long MAX_ORDER = 0x00_FF_FF_FF_FF_FF_FF_FFL;

        int[] topVers = {0, 1, 100, 256, MAX_TOP_VER};
        int[] minorTopVers = {0, 1, 100, 256, MAX_MINOR_VER};
        long[] orders = {0, 1, 100, 256, MAX_ORDER};

        for (int topVer : topVers) {
            for (int minorTopVer : minorTopVers) {
                for (long order : orders)
                    checkNewVersion(topVer, minorTopVer, order);
            }
        }

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 1000; i++) {
            checkNewVersion(rnd.nextInt(0, MAX_TOP_VER + 1),
                rnd.nextInt(0, MAX_MINOR_VER),
                rnd.nextLong(0, MAX_ORDER + 1));
        }
    }

    /**
     * Test versions marshalling.
     *
     * @throws Exception If failed.
     */
    public void testMarshalling() throws Exception {
        GridCacheVersion ver = (GridCacheVersion)version(1, 1, false);
        GridCacheVersionEx verEx = new GridCacheVersionEx(2, 2, 0, 0, ver);

        OptimizedMarshaller marsh = new OptimizedMarshaller(false);

        marsh.setContext(new MarshallerContextTestImpl());

        byte[] verBytes = marsh.marshal(ver);
        byte[] verExBytes = marsh.marshal(verEx);

        GridCacheVersion verNew = marsh.unmarshal(verBytes, Thread.currentThread().getContextClassLoader());
        GridCacheVersionEx verExNew = marsh.unmarshal(verExBytes, Thread.currentThread().getContextClassLoader());

        assert ver.equals(verNew);
        assert verEx.equals(verExNew);
    }

    /**
     * Test versions marshalling.
     *
     * @throws Exception If failed.
     */
    public void testMarshallingNewVer() throws Exception {
        CacheVersionImpl ver = (CacheVersionImpl)version(1, 1, true);
        CacheVersionImplEx verEx = new CacheVersionImplEx(2, 2, 0, 0, ver);

        OptimizedMarshaller marsh = new OptimizedMarshaller(false);

        marsh.setContext(new MarshallerContextTestImpl());

        byte[] verBytes = marsh.marshal(ver);
        byte[] verExBytes = marsh.marshal(verEx);

        CacheVersionImpl verNew = marsh.unmarshal(verBytes, Thread.currentThread().getContextClassLoader());
        CacheVersionImplEx verExNew = marsh.unmarshal(verExBytes, Thread.currentThread().getContextClassLoader());

        assert ver.equals(verNew);
        assert verEx.equals(verExNew);
    }

    /**
     * @param nodeOrder Node order.
     * @param drId Data center ID.
     * @param newVer If {@code true} creates {@link CacheVersionImpl}.
     * @return Cache version.
     */
    private CacheVersion version(int nodeOrder, int drId, boolean newVer) {
        return newVer? new CacheVersionImpl(0, 0, 0, 0, nodeOrder, drId) :
            new GridCacheVersion(0, 0, 0, nodeOrder, drId);
    }

    /**
     * @param topVer Topology version.
     * @param minorTopVer Minor topology version.
     * @param order Order.
     */
    private void checkNewVersion(int topVer, int minorTopVer, long order) {
        CacheVersion ver = newVersion(topVer, minorTopVer, order);

        assertEquals(topVer, ver.topologyVersion());
        assertEquals(minorTopVer, ver.minorTopologyVersion());
        assertEquals(order, ver.order());
    }

    /**
     * @param topVer Topology version.
     * @param minorTopVer Minor topology version.
     * @param order Order.
     * @return Cache version.
     */
    private CacheVersion newVersion(int topVer, int minorTopVer, long order) {
        return new CacheVersionImpl(topVer, minorTopVer, 0, order, 0, 0);
    }
}