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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.custom.DummyEventFilterFactory;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks that Optimized Marshaller is not used on any stage of Continuous Query handling.
 */
public class ContinuousQueryMarshallerTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(gridName.contains("client"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteFilterFactoryClient() throws Exception {
        check("server", "client");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteFilterFactoryServer() throws Exception {
        check("server1", "server2");
    }

    /**
     * @param node1Name Node 1 name.
     * @param node2Name Node 2 name.
     */
    private void check(String node1Name, String node2Name) throws Exception {
        final Ignite node1 = startGrid(node1Name);

        final IgniteCache<Integer, MarshallerCheckingEntry> cache = node1.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, new MarshallerCheckingEntry(String.valueOf(i)));

        final Ignite node2 = startGrid(node2Name);

        final ContinuousQuery<Integer, MarshallerCheckingEntry> qry = new ContinuousQuery<>();

        ScanQuery<Integer, MarshallerCheckingEntry> scanQry = new ScanQuery<>(new IgniteBiPredicate<Integer, MarshallerCheckingEntry>() {
            @Override public boolean apply(Integer key, MarshallerCheckingEntry val) {
                return key % 2 == 0;
            }
        });

        qry.setInitialQuery(scanQry);

        qry.setRemoteFilterFactory(new DummyEventFilterFactory<>());

        final CountDownLatch latch = new CountDownLatch(15);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, MarshallerCheckingEntry>() {
            @Override public void onUpdated(
                final Iterable<CacheEntryEvent<? extends Integer, ? extends MarshallerCheckingEntry>> evts)
                throws CacheEntryListenerException {

                System.out.println(">> Client 1 events " + evts);

                for (CacheEntryEvent<? extends Integer, ? extends MarshallerCheckingEntry> evt : evts)
                    latch.countDown();
            }
        });

        final IgniteCache<Integer, MarshallerCheckingEntry> cache1 = node2.cache(CACHE_NAME);

        for (Cache.Entry<Integer, MarshallerCheckingEntry> entry : cache1.query(qry)) {
            latch.countDown();
            System.out.println(">> Initial entry " + entry);
        }

        for (int i = 10; i < 20; i++)
            cache1.put(i, new MarshallerCheckingEntry(i));

        assertTrue(Long.toString(latch.getCount()), latch.await(5, TimeUnit.SECONDS));
    }

    /** Checks that OptimizedMarshaller is never used (BinaryMarshaller is OK) */
    private class MarshallerCheckingEntry implements Serializable, Binarylizable {
        /** */
        private Object val;

        /** */
        public MarshallerCheckingEntry(Object val) {
            this.val = val;
        }

        /** */
        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new UnsupportedOperationException();
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            throw new UnsupportedOperationException();
        }

        /** */
        private void readObjectNoData() throws ObjectStreamException {
            throw new UnsupportedOperationException();
        }

        /** */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("value", val);
        }

        /** */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readObject("value");
        }
    }

}
