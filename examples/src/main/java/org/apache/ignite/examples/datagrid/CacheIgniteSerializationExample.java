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

package org.apache.ignite.examples.datagrid;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class CacheIgniteSerializationExample {
    /** Cache name. */
    private static final String CACHE_NAME = CachePutGetExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

            if (args != null) {

                CacheConfiguration<Integer, IgniteHolder> cacheCfg = new CacheConfiguration<>();

                cacheCfg.setCacheMode(CacheMode.PARTITIONED);
                cacheCfg.setBackups(1);
                cacheCfg.setName("cacheCfg");

                IgniteCache<Integer, IgniteHolder> cache = ignite.getOrCreateCache(cacheCfg);

                System.out.println(ignite.name());

                IgniteHolder ignite0 = new IgniteHolder(ignite);
                IgniteHolder ignite1 = new IgniteHolder(ignite);

                cache.put(1, ignite1);
                cache.put(2, ignite0);
                cache.put(217, ignite0);

                ignite.getOrCreateCache("cacheCfg").get(1);
                ignite.getOrCreateCache("cacheCfg").get(2);
            }
        }
    }

    public static class IgniteHolder implements Externalizable {

       /* @IgniteInstanceResource
        private Ignite ignite;

        private String gridName;

        public IgniteHolder(Ignite ignite) {
            this.ignite = ignite;
            this.gridName = ignite.name();
        }

        public Ignite getIgnite() {
            return ignite;
        }*/

        private Ignite ignite;
        private String gridName;

        public IgniteHolder() {}

        public IgniteHolder(Ignite ignite) {
            this.ignite = ignite;
            this.gridName = ignite.name();
        }

        public Ignite getIgnite() {
            return ignite;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            gridName = U.readString(in);
            ignite = IgnitionEx.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, gridName);
        }

        /**
         * @return IgniteKernal instance.
         *
         * @throws ObjectStreamException If failed.
         */
        protected Object readResolve() throws ObjectStreamException {
            try {
                return new IgniteHolder(IgnitionEx.localIgnite());
            }
            catch (IllegalStateException e) {
                throw U.withCause(new InvalidObjectException(e.getMessage()), e);
            }
        }

    }

}