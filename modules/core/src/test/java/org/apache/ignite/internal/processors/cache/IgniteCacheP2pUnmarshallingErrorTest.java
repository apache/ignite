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
 * Check behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingErrorTest extends IgniteCacheAbstractTest {
    /** Allows to change behavior of readExternal method. */
    protected static AtomicInteger readCnt = new AtomicInteger();

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

    /** Test key 1. */
    public static class TestKey implements Externalizable {
        /** Test key 1. */
        public TestKey(String field) {
            this.field = field;
        }

        /** Test key 1. */
        public TestKey() {
        }

        /** field. */
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
            out.writeObject(field);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            field = (String)in.readObject();

            if (readCnt.decrementAndGet() <= 0) { //will throw exception on backup node only
                throw new IOException("Class can not be unmarshalled");
            }
        }
    }

    /**
     * Sends put atomically and handles fail.
     */
    protected void failAtomicPut() {
        try {
            jcache(0).put(new TestKey("1"), "");

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }

        assert readCnt.get() == 0; //ensure we have read count as expected.
    }

    /**
     * Sends get atomically and handles fail.
     */
    protected void failAtomicGet() {
        try {
            jcache(0).get(new TestKey("1"));

            assert false : "p2p marshalling failed, but error response was not sent";
        }
        catch (CacheException e) {
            assert X.hasCause(e, IOException.class);
        }
    }

    /**
     * Tests that correct response will be sent to client node in case of unmarshalling failed.
     */
    public void testResponseMessageOnUnmarshallingFailed() {
        //GridNearAtomicUpdateRequest unmarshalling failed test
        readCnt.set(1);

        failAtomicPut();

        //GridNearGetRequest unmarshalling failed test
        readCnt.set(1);

        failAtomicGet();

        readCnt.set(100);

        assert jcache(0).get(new TestKey("1")) == null;

        readCnt.set(2);

        //GridDhtAtomicUpdateRequest unmarshalling failed test
        failAtomicPut();

        readCnt.set(100);

        assert jcache(0).get(new TestKey("1")) != null;
    }
}
