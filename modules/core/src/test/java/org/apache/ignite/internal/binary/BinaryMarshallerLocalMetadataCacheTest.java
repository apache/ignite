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

package org.apache.ignite.internal.binary;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests IGNITE_USE_LOCAL_BINARY_MARSHALLER_CACHE property.
 */
public class BinaryMarshallerLocalMetadataCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName,
        IgniteTestResources rsrcs) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName, rsrcs);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientMode(gridName.startsWith("client"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_USE_LOCAL_BINARY_MARSHALLER_CACHE, "true");

        startGrid(0);
        startGrid(1);
        startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            stopAllGrids();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_USE_LOCAL_BINARY_MARSHALLER_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testComputeLocalMetadata() throws Exception {
        final BinaryObject obj = grid(0).binary().toBinary(new OptimizedContainer(new Optimized()));

        ClusterGroup remotes = grid(0).cluster().forRemotes();

        OptimizedContainer res = grid(0).compute(remotes).call(new IgniteCallable<OptimizedContainer>() {
            @Override public OptimizedContainer call() throws Exception {

                return obj.deserialize();
            }
        });

        OptimizedContainer res2 = grid(0).compute(remotes).call(new IgniteCallable<OptimizedContainer>() {
            @Override public OptimizedContainer call() throws Exception {

                return obj.deserialize();
            }
        });

        System.out.println(res);
        System.out.println(res2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheLocalMetadata() throws Exception {
        IgniteCache<Key, Object> cache = grid("client").createCache("cache");

        Map<Key, Object> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(new Key(i), new OptimizedContainer(new Optimized(String.valueOf(i))));

        for (int i = 1000; i < 2000; i++)
            data.put(new Key(i), new Simple(i, String.valueOf(i), new BigInteger(String.valueOf(i), 10)));

        cache.putAll(data);

        checkCache(cache, data);
        checkCache(grid(0).<Key, Object>cache("cache"), data);
        checkCache(grid(1).<Key, Object>cache("cache"), data);
    }

    /**
     * @param cache Cache.
     * @param data Data.
     */
    private void checkCache(IgniteCache<Key, Object> cache,
        Map<Key, Object> data) {
        for (Map.Entry<Key, Object> entry : cache.getAll(data.keySet()).entrySet())
            assertEquals(data.get(entry.getKey()), entry.getValue());
    }

    /**
     *
     */
    private static class Key {
        /** */
        private Integer i;

        /**
         * @param i I.
         */
        public Key(Integer i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return i != null ? i.equals(key.i) : key.i == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return i != null ? i.hashCode() : 0;
        }
    }

    /**
     *
     */
    private static class OptimizedContainer {
        /** */
        private Optimized optim;

        /**
         * @param optim Val.
         */
        public OptimizedContainer(Optimized optim) {
            this.optim = optim;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            OptimizedContainer container = (OptimizedContainer)o;

            return optim != null ? optim.equals(container.optim) : container.optim == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return optim != null ? optim.hashCode() : 0;
        }
    }

    /**
     *
     */
    private static class Optimized implements Externalizable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private String fld;

        /**
         * @param fld Fld.
         */
        public Optimized(String fld) {
            this.fld = fld;
        }

        /**
         * Default constructor (required by Externalizable).
         */
        public Optimized() {
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUTFStringNullable(out, fld);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fld = U.readUTFStringNullable(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Optimized optimized = (Optimized)o;

            return fld != null ? fld.equals(optimized.fld) : optimized.fld == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return fld != null ? fld.hashCode() : 0;
        }
    }

    /**
     *
     */
    private static class Simple {
        /** I. */
        private int i;

        /** String. */
        private String str;

        /** Big integer. */
        private BigInteger bigInteger;

        /**
         * @param i I.
         * @param str String.
         * @param bigInteger Big integer.
         */
        public Simple(int i, String str, BigInteger bigInteger) {
            this.i = i;
            this.str = str;
            this.bigInteger = bigInteger;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Simple simple = (Simple)o;

            if (i != simple.i)
                return false;

            if (str != null ? !str.equals(simple.str) : simple.str != null)
                return false;

            return bigInteger != null ? bigInteger.equals(simple.bigInteger) : simple.bigInteger == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = i;

            res = 31 * res + (str != null ? str.hashCode() : 0);
            res = 31 * res + (bigInteger != null ? bigInteger.hashCode() : 0);

            return res;
        }
    }
}
