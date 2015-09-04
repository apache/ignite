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
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingErrorTest extends IgniteCacheAbstractTest {
    /** Allows to change behavior of readExternal method. */
    protected static AtomicInteger readCnt = new AtomicInteger();

    /** Iterable key. */
    protected static int key = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1366");
    }

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
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(0).equals(gridName)) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration();
        }

        return cfg;
    }

    /** Test key 1. */
    public static class TestKey implements Externalizable {
        /** Field. */
        @QuerySqlField(index = true)
        private String field;

        /**
         * @param field Test key 1.
         */
        public TestKey(String field) {
            this.field = field;
        }

        /** Test key 1. */
        public TestKey() {
            // No-op.
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
    protected void failAtomicGet(int k) {
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
    public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        //GridNearAtomicUpdateRequest unmarshalling failed test
        readCnt.set(1);

        failAtomicPut(++key);

        //Check that cache is empty.
        readCnt.set(Integer.MAX_VALUE);

        assert jcache(0).get(new TestKey(String.valueOf(key))) == null;

        //GridDhtAtomicUpdateRequest unmarshalling failed test
        readCnt.set(2);

        failAtomicPut(++key);

        //Check that cache is not empty.
        readCnt.set(Integer.MAX_VALUE);

        assert jcache(0).get(new TestKey(String.valueOf(key))) != null;

        //GridNearGetRequest unmarshalling failed test
        readCnt.set(1);

        failAtomicGet(++key);

        //GridNearGetResponse unmarshalling failed test
        readCnt.set(Integer.MAX_VALUE);

        jcache(0).put(new TestKey(String.valueOf(++key)), "");

        readCnt.set(2);

        failAtomicGet(key);
    }
}