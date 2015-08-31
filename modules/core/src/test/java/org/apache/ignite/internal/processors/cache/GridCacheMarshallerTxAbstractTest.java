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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test transaction with wrong marshalling.
 */
public abstract class GridCacheMarshallerTxAbstractTest extends GridCommonAbstractTest {
    /**
     * Wrong Externalizable class.
     */
    private static class GridCacheWrongValue implements Externalizable {
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new NullPointerException("Expected exception.");
        }

        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new NullPointerException("Expected exception.");
        }
    }

        /**
     * Wrong Externalizable class.
     */
    private static class GridCacheWrongValue1 {
        private int val1 = 8;
        private long val2 = 9;
    }

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Constructs a test.
     */
    protected GridCacheMarshallerTxAbstractTest() {
        super(true /* start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testValueMarshallerFail() throws Exception {
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        String newValue = UUID.randomUUID().toString();

        String key2 = UUID.randomUUID().toString();
        GridCacheWrongValue1 wrongValue = new GridCacheWrongValue1();

        Transaction tx = grid().transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
        try {
            grid().cache(null).put(key, value);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = grid().transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        try {
            assert value.equals(grid().cache(null).get(key));

            grid().cache(null).put(key, newValue);

            grid().cache(null).put(key2, wrongValue);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = grid().transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        try {
            String locVal = (String)grid().cache(null).get(key);

            assert locVal != null;

            tx.commit();
        }
        finally {
            tx.close();
        }
    }
}