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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingErrorTest extends IgniteCacheAbstractTest {
    /** Allows to change behavior of readExternal method. */
    protected static final AtomicInteger readCnt = new AtomicInteger();

    /** Allows to change behavior of readExternal method. */
    protected static final AtomicInteger valReadCnt = new AtomicInteger();

    /** Iterable key. */
    protected static int key = 0;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void startGrids() throws Exception {
        int cnt = gridCount();

        assert cnt >= 1 : "At least one grid must be started";

        startGridsMultiThreaded(1, cnt - 1);

        startClientGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setCacheConfiguration();

        if (getTestIgniteInstanceName(10).equals(igniteInstanceName)) {
            CacheConfiguration cc = cfg.getCacheConfiguration()[0];
            cc.setRebalanceDelay(-1);
        }

        return cfg;
    }

    /**
     * Sends put atomically and handles fail.
     *
     * @param k Key.
     */
    protected void failAtomicPut(int k) {
        try {
            jcache(0).put(new TestKey(String.valueOf(k)), "");

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert readCnt.get() == 0; //ensure we have read count as expected.
    }

    /**
     * Sends get atomically and handles fail.
     *
     * @param k Key.
     */
    protected void failGetAll(int k) {
        try {
            Set<Object> keys = F.<Object>asSet(new TestKey(String.valueOf(k)));

            jcache(0).getAll(keys);

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }
    }

    /**
     * Sends get atomically and handles fail.
     *
     * @param k Key.
     */
    protected void failGet(int k) {
        try {
            jcache(0).get(new TestKey(String.valueOf(k)));

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }
    }

    /**
     * Tests that correct response will be sent to client node in case of unmarshalling failed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        // GridNearAtomicFullUpdateRequest unmarshalling failed test.
        readCnt.set(1);

        failAtomicPut(++key);

        // Check that cache is empty.
        readCnt.set(Integer.MAX_VALUE);

        assert jcache(0).get(new TestKey(String.valueOf(key))) == null;

        // GridDhtAtomicUpdateRequest unmarshalling failed test.
        readCnt.set(2);

        failAtomicPut(++key);

        // Check that cache is not empty.
        readCnt.set(Integer.MAX_VALUE);

        assert jcache(0).get(new TestKey(String.valueOf(key))) != null;

        // GridNearGetRequest unmarshalling failed test.
        readCnt.set(1);

        failGetAll(++key);

        // GridNearGetResponse unmarshalling failed test.
        readCnt.set(Integer.MAX_VALUE);

        jcache(0).put(new TestKey(String.valueOf(++key)), "");

        readCnt.set(2);

        failGetAll(key);

        readCnt.set(Integer.MAX_VALUE);
        valReadCnt.set(Integer.MAX_VALUE);

        jcache(0).put(new TestKey(String.valueOf(++key)), new TestValue());

        assertNotNull(new TestKey(String.valueOf(key)));

        // GridNearSingleGetRequest unmarshalling failed.
        readCnt.set(1);

        failGet(key);

        // GridNearSingleGetRequest unmarshalling failed.
        valReadCnt.set(1);
        readCnt.set(2);

        failGet(key);
    }

    /**
     * Test key.
     */
    protected static class TestKey implements Externalizable {
        /** Field. */
        @QuerySqlField(index = true)
        private String field;

        /**
         * Required by {@link Externalizable}.
         */
        public TestKey() {
            // No-op.
        }

        /**
         * @param field Test key 1.
         */
        public TestKey(String field) {
            this.field = field;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestKey key = (TestKey)o;

            return !(field != null ? !field.equals(key.field) : key.field != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return field != null ? field.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(field);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            field = (String)in.readObject();

            if (readCnt.decrementAndGet() <= 0)
                throw new IOException("Class can not be unmarshalled.");
        }
    }

    /**
     * Test value.
     */
    protected static class TestValue implements Externalizable {
        /**
         * Required by {@link Externalizable}.
         */
        public TestValue() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            if (valReadCnt.decrementAndGet() <= 0)
                throw new IOException("Class can not be unmarshalled.");
        }
    }
}
