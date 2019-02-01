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

package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class TransactionsMXBeanImplTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        final CacheConfiguration cCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testBasic() throws Exception {
        IgniteEx ignite = startGrid(0);

        TransactionsMXBean bean = txMXBean(0);

        ignite.transactions().txStart();

        ignite.cache(DEFAULT_CACHE_NAME).put(0, 0);

        String res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, false);

        assertEquals("1", res);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, true, false);

        assertTrue(res.indexOf("Tx:") > 0);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, true);

        assertEquals("1", res);

        doSleep(500);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, false);

        assertEquals("0", res);
    }

    /**
     *
     */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TransactionsMXBean.class, true);
    }
}
