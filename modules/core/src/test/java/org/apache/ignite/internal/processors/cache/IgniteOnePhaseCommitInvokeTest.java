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
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteOnePhaseCommitInvokeTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOnePhaseInvoke() throws Exception {
        boolean flags[] = {true, false};

        for (boolean withOldVal : flags) {
            for (boolean setVal : flags) {
                for (boolean retPrev : flags) {
                    onePhaseInvoke(withOldVal, setVal, retPrev);

                    stopAllGrids();
                }
            }
        }
    }

    /**
     * @param withOldVal If {@code true}
     * @param setVal Flag whether set value from entry processor.
     * @param retPrev Flag whether entry processor should return previous value.
     * @throws Exception If failed.
     */
    private void onePhaseInvoke(final boolean withOldVal,
        final boolean setVal,
        final boolean retPrev)
        throws Exception
    {
        log.info("Test onePhaseInvoke [withOldVal=" + withOldVal + ", setVal=" + setVal + ", retPrev=" + retPrev + ']');

        Ignite srv0 = startGrid(0);

        if (withOldVal)
            srv0.cache(CACHE_NAME).put(1, 1);

        final Ignite clientNode = startClientGrid(1);

        final int grpId = groupIdForCache(srv0, CACHE_NAME);

        TestRecordingCommunicationSpi.spi(srv0).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage &&
                    ((GridDhtPartitionSupplyMessage)msg).groupId() == grpId;
            }
        });

        Ignite srv1 = startGrid(2);

        TestRecordingCommunicationSpi.spi(srv1).blockMessages(GridDhtTxPrepareResponse.class, srv0.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Object res = clientNode.cache(CACHE_NAME).invoke(1, new TestEntryProcessor(setVal, retPrev));

                Object expRes;

                if (retPrev)
                    expRes = withOldVal ? 1 : null;
                else
                    expRes = null;

                assertEquals(expRes, res);

                return null;
            }
        });

        U.sleep(1000);

        stopGrid(0);

        fut.get();

        if (!setVal)
            checkCacheData(F.asMap(1, null), CACHE_NAME);
        else {
            Object expVal;

            if (setVal)
                expVal = 2;
            else
                expVal = withOldVal ? 1 : null;

            checkCacheData(F.asMap(1, expVal), CACHE_NAME);
        }

        checkOnePhaseCommitReturnValuesCleaned(-1);
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor {
        /** */
        private final boolean setVal;

        /** */
        private final boolean retPrev;

        /**
         * @param setVal Set value flag.
         * @param retPrev Return previous value flag.
         */
        TestEntryProcessor(boolean setVal, boolean retPrev) {
            this.setVal = setVal;
            this.retPrev = retPrev;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry e, Object... args) {
            Object val = e.getValue();

            if (setVal)
                e.setValue(2);

            return retPrev ? val : null;
        }
    }
}
