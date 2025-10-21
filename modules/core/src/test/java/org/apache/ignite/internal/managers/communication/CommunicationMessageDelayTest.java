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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class CommunicationMessageDelayTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCommunicationSpi(new DelayingTcpCommunicationSpi());
    }

    /** */
    @Test
    public void testTxConsistencyAfterTimeout1PC() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx client = startClientGrid(1);

        checkTx(client, srv, PESSIMISTIC, REPEATABLE_READ);
        checkTx(client, srv, OPTIMISTIC, REPEATABLE_READ);
        checkTx(client, srv, OPTIMISTIC, SERIALIZABLE);
    }

    /** */
    private void checkTx(
        IgniteEx client,
        IgniteEx srv,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation
    ) {
        long txTimout = 1_000L;

        IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        Transaction tx = client.transactions().txStart(txConcurrency, txIsolation, txTimout, 0);

        cache.put(0, 0);

        DelayingTcpCommunicationSpi.delay(srv, GridNearTxPrepareResponse.class, 2 * txTimout);

        try {
            tx.commit();

            assertTrue(cache.containsKey(0));
        }
        catch (Exception e) {
            assertFalse(cache.containsKey(0));
        }

        cache.remove(0);
    }

    /** */
    public static class DelayingTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private Class<?> delayMsg;

        /** */
        private long delayTime;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackClosure
        ) throws IgniteSpiException {
            if (delayMsg != null && ((GridIoMessage)msg).message().getClass().equals(delayMsg))
                doSleep(delayTime);

            super.sendMessage(node, msg, ackClosure);
        }

        /** */
        public void delay(Class<?> delayMsg, long delayTime) {
            this.delayMsg = delayMsg;
            this.delayTime = delayTime;
        }

        /** */
        public static void delay(Ignite ignite, Class<?> delayMsg, long delayTime) {
            ((DelayingTcpCommunicationSpi)ignite.configuration().getCommunicationSpi()).delay(delayMsg, delayTime);
        }
    }
}
