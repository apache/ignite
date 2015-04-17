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

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;

import javax.cache.*;
import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Check behavior on exception while unmarshalling key
 */
public class IgniteCacheP2pUnmarshallingErrorTest extends IgniteCacheAbstractTest {
    /** Allows to change behavior of readExternal method */
    private static AtomicInteger nodeCnt = new AtomicInteger();

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

        if (gridName.endsWith("0"))
            cfg.setClientMode(true);

        return cfg;
    }

    /** Test key 1 */
    public static class TestKey implements Externalizable {
        /** Test key 1 */
        public TestKey(String field) {
            this.field = field;
        }

        /** Test key 1 */
        public TestKey() {
        }

        /** field */
        private String field;

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

        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            if (nodeCnt.decrementAndGet() < 1) { //will throw exception on backup node only
                throw new IOException("Class can not be unmarshalled");
            }
        }
    }

    /**
     * Test key 2.
     * Unmarshalling always failed.
     */
    public static class TestKeyAlwaysFailed extends TestKey {
        /** Test key 2 */
        public TestKeyAlwaysFailed(String field) {
            super(field);
        }

        /** Test key 2 */
        public TestKeyAlwaysFailed() {
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            nodeCnt.decrementAndGet();
            throw new IOException("Class can not be unmarshalled"); //will throw exception on primary node
        }

    }

    /**
     * Tests that correct response will be sent to client node in case of unmarshalling failed.
     */
    public void testResponseMessageOnUnmarshallingFailed() {

        nodeCnt.set(1);

        try {
            jcache(0).put(new TestKeyAlwaysFailed("1"), "");
            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert nodeCnt.get() == 0;//put request should not go to backup node in case failed at primary.

        try {
            assert jcache(0).get(new TestKeyAlwaysFailed("1")) == null;
            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert grid(0).cachex().entrySet().size() == 0;

        nodeCnt.set(2); //put request will be unmarshalled twice (at primary and at backup node).

        try {
            jcache(0).put(new TestKey("1"), "");//put will fail at backup node.
            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert nodeCnt.get() == 0;//put request should go to primary and backup node.

        // Need to have to exception while unmarshalling getKeyResponse.
        nodeCnt.set(3); //get response will me unmarshalled twice (request at primary node and response at client).

        assert jcache(0).get(new TestKey("1")) == null;

        assert grid(0).cachex().entrySet().size() == 0;
    }
}
