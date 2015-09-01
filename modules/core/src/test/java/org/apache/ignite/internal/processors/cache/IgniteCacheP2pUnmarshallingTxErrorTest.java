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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingTxErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (!gridName.endsWith("0"))
            cfg.getCacheConfiguration()[0].setRebalanceDelay(-1); // Allows to check GridDhtLockRequest fail.

        return cfg;
    }

    /**
     * Sends put with optimistic lock and handles fail.
     */
    protected void failOptimistic() {
        IgniteCache<Object, Object> cache = jcache(0);

        try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {

            cache.put(new TestKey(String.valueOf(++key)), "");

            tx.commit();

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (IgniteException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert readCnt.get() == 0; // Ensure we have read count as expected.
    }

    /**
     * Sends put with pessimistic lock and handles fail.
     */
    protected void failPessimictic() {
        IgniteCache<Object, Object> cache = jcache(0);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC,
            REPEATABLE_READ)) {

            cache.put(new TestKey(String.valueOf(++key)), "");

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert readCnt.get() == 0; // Ensure we have read count as expected.
    }

    /** {@inheritDoc} */
    @Override public void testResponseMessageOnUnmarshallingFailed() {
        //GridNearTxPrepareRequest unmarshalling failed test
        readCnt.set(2);

        failOptimistic();

        //GridDhtTxPrepareRequest unmarshalling failed test
        readCnt.set(3);

        failOptimistic();

        //GridNearLockRequest unmarshalling failed test
        readCnt.set(2);

        failPessimictic();

        //GridDhtLockRequest unmarshalling failed test
        readCnt.set(3);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            jcache(0).put(new TestKey(String.valueOf(++key)), ""); //No failure at client side.
        }
    }
}