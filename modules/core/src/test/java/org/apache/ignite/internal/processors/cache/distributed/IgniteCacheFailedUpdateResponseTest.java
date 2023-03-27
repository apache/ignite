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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Checks that no future hangs on non-serializable exceptions and values.
 */
public class IgniteCacheFailedUpdateResponseTest extends GridCommonAbstractTest {
    /** Atomic cache. */
    private static final String ATOMIC_CACHE = "atomic";

    /** Tx cache. */
    private static final String TX_CACHE = "tx";

    /** Mvcc tx cache. */
    private static final String MVCC_TX_CACHE = "mvcc-tx";

    /** Atomic cache. */
    private IgniteCache<Object, Object> atomicCache;

    /** Tx cache. */
    private IgniteCache<Object, Object> txCache;

    /** Mvcc tx cache. */
    private IgniteCache<Object, Object> mvccTxCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration atomicCfg = new CacheConfiguration(ATOMIC_CACHE);
        CacheConfiguration txCfg = new CacheConfiguration(TX_CACHE);
        CacheConfiguration mvccTxCfg = new CacheConfiguration(MVCC_TX_CACHE);

        atomicCfg.setBackups(1);
        txCfg.setBackups(1);
        mvccTxCfg.setBackups(1);

        txCfg.setAtomicityMode(TRANSACTIONAL);
        mvccTxCfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        cfg.setCacheConfiguration(atomicCfg, txCfg, mvccTxCfg);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(LOCAL_IP_FINDER);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server-1");
        startGrid("server-2");
        startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        atomicCache = grid("client").cache(ATOMIC_CACHE);
        txCache = grid("client").cache(TX_CACHE);
        mvccTxCache = grid("client").cache(MVCC_TX_CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAtomic() throws Exception {
        testInvoke(atomicCache);
        testInvokeAll(atomicCache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeTx() throws Exception {
        testInvoke(txCache);
        testInvokeAll(txCache);

        IgniteEx client = grid("client");

        Callable<Object> clos = new Callable<Object>() {
            @Override public Object call() throws Exception {
                testInvoke(txCache);
                testInvokeAll(txCache);

                return null;
            }
        };

        doInTransaction(client, PESSIMISTIC, READ_COMMITTED, clos);
        doInTransaction(client, PESSIMISTIC, REPEATABLE_READ, clos);
        doInTransaction(client, PESSIMISTIC, SERIALIZABLE, clos);
        doInTransaction(client, OPTIMISTIC, READ_COMMITTED, clos);
        doInTransaction(client, OPTIMISTIC, REPEATABLE_READ, clos);
        doInTransaction(client, OPTIMISTIC, SERIALIZABLE, clos);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeMvccTx() throws Exception {
        testInvoke(mvccTxCache);
        testInvokeAll(mvccTxCache);

        IgniteEx client = grid("client");

        Callable<Object> clos = new Callable<Object>() {
            @Override public Object call() throws Exception {
                testInvoke(mvccTxCache);
                testInvokeAll(mvccTxCache);

                return null;
            }
        };

        doInTransaction(client, PESSIMISTIC, REPEATABLE_READ, clos);
    }

    /**
     * @param cache Cache.
     */
    private void testInvoke(final IgniteCache<Object, Object> cache) throws Exception {
        Class<? extends Exception> exp = grid("client").transactions().tx() == null || ((IgniteCacheProxy)cache).context().mvccEnabled()
            ? EntryProcessorException.class
            : NonSerializableException.class;

        //noinspection ThrowableNotThrown
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.invoke("1", new UpdateProcessor());

                return null;
            }
        }, exp, null);

        if (ATOMIC_CACHE.equals(cache.getName())) {
            //noinspection ThrowableNotThrown
            assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    cache.invoke("1", new UpdateValueProcessor());

                    return null;
                }
            }, CachePartialUpdateException.class, null);
        }
    }

    /**
     * @param cache Cache.
     */
    private void testInvokeAll(final IgniteCache<Object, Object> cache) throws Exception {
        Map<Object, EntryProcessorResult<Object>> results = cache.invokeAll(Collections.singleton("1"), new UpdateProcessor());

        final EntryProcessorResult<Object> epRes = F.first(results.values());

        assertNotNull(epRes);

        // In transactions EP will be invoked locally.
        Class<? extends Exception> exp = grid("client").transactions().tx() == null || ((IgniteCacheProxy)cache).context().mvccEnabled()
            ? EntryProcessorException.class
            : NonSerializableException.class;

        //noinspection ThrowableNotThrown
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                epRes.get();

                return null;
            }
        }, exp, null);

        if (ATOMIC_CACHE.equals(cache.getName())) {
            //noinspection ThrowableNotThrown
            assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    cache.invokeAll(Collections.singleton("1"), new UpdateValueProcessor());

                    return null;
                }
            }, CachePartialUpdateException.class, null);
        }
    }

    /**
     *
     */
    private static class Value implements Externalizable, Binarylizable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public Value() {
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new NotSerializableException("Test marshalling exception");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            throw new BinaryObjectException("Test marshalling exception");
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            // No-op.
        }
    }

    /**
     *
     */
    private static class NonSerializableException extends EntryProcessorException implements Externalizable, Binarylizable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public NonSerializableException() {
            super();
        }

        /**
         * @param msg Message.
         */
        NonSerializableException(String msg) {
            super(msg);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new NotSerializableException("Test marshalling exception");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            throw new BinaryObjectException("Test marshalling exception");
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            // No-op.
        }
    }

    /**
     *
     */
    private static class UpdateProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry,
            Object... arguments) throws EntryProcessorException {
            throw new NonSerializableException("Test exception");
        }
    }

    /**
     *
     */
    private static class UpdateValueProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry,
            Object... arguments) throws EntryProcessorException {
            return new Value();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20_000;
    }
}
