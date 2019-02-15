/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

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

    /**
     * Sends put with optimistic lock and handles fail.
     */
    private void failOptimistic() {
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
    private void failPessimictic() {
        IgniteCache<Object, Object> cache = jcache(0);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(new TestKey(String.valueOf(++key)), "");

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert readCnt.get() == 0; // Ensure we have read count as expected.
    }

    /** {@inheritDoc} */
    @Test
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
