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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheInterceptorDeserializeAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

public abstract class CacheInterceptorClientsAbstractTest extends GridCommonAbstractTest {

    protected static final String SERVER_NODE_NAME = "server-node";

    protected static final String ATOMIC_CACHE_NAME_REPLICATED = "atomic-cache-rep";

    protected static final String ATOMIC_CACHE_NAME_PARTITIONED = "atomic-cache-part";

    protected static final String ATOMIC_CACHE_NAME_LOCAL = "atomic-cache-loc";

    protected static final String TX_CACHE_NAME_REPLICATED = "tx-cache-rep";

    protected static final String TX_CACHE_NAME_PARTITIONED = "tx-cache-part";

    protected static final String TX_CACHE_NAME_LOCAL = "tx-cache-loc";

    protected static final Collection<String> CACHE_NAMES;

    static {
        Set<String> set = new HashSet<>();

        set.add(ATOMIC_CACHE_NAME_REPLICATED);
        set.add(ATOMIC_CACHE_NAME_PARTITIONED);
        set.add(ATOMIC_CACHE_NAME_LOCAL);

        set.add(TX_CACHE_NAME_REPLICATED);
        set.add(TX_CACHE_NAME_PARTITIONED);
        set.add(TX_CACHE_NAME_LOCAL);

        CACHE_NAMES = Collections.unmodifiableCollection(set);
    }

    protected Ignite thickClient;

    protected IgniteClient thinClient;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());

        cfg.setCacheConfiguration(
            createCacheCfg(ATOMIC_CACHE_NAME_REPLICATED, REPLICATED, ATOMIC),
            createCacheCfg(ATOMIC_CACHE_NAME_PARTITIONED, PARTITIONED, ATOMIC),
            createCacheCfg(ATOMIC_CACHE_NAME_LOCAL, LOCAL, ATOMIC),

            createCacheCfg(TX_CACHE_NAME_REPLICATED, REPLICATED, TRANSACTIONAL),
            createCacheCfg(TX_CACHE_NAME_PARTITIONED, PARTITIONED, TRANSACTIONAL),
            createCacheCfg(TX_CACHE_NAME_LOCAL, LOCAL, TRANSACTIONAL)
        );

        return cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteConfiguration thickClientCfg = getConfiguration("thick-client").setClientMode(true);

        startGrid(SERVER_NODE_NAME);

        thickClient = startGrid(thickClientCfg);

        thickClient.cluster().active(true);

        ClientConnectorConfiguration ccfg = grid(SERVER_NODE_NAME).configuration().getClientConnectorConfiguration();

        ClientConfiguration thinClientCfg = new ClientConfiguration().setAddresses("127.0.0.1:" + ccfg.getPort());

        thinClient = Ignition.startClient(thinClientCfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        populateData();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CACHE_NAMES.forEach(c -> grid(SERVER_NODE_NAME).cache(c).clear());
    }

    protected ClientCache thinCache(String cacheName) {
        if(binary())
            return thinClient.cache(cacheName).withKeepBinary();
        else
            return thinClient.cache(cacheName);
    }

    protected IgniteCache thickCache(String cacheName) {
        if(binary())
            return thickClient.cache(cacheName).withKeepBinary();
        else
            return thickClient.cache(cacheName);
    }

    protected CacheConfiguration createCacheCfg(String name, CacheMode cMode, CacheAtomicityMode aMode) {
        CacheConfiguration cfg = new CacheConfiguration().setName(name).setCacheMode(cMode).setAtomicityMode(aMode);

        return cfg.setInterceptor(binary() ?  new NoopBinaryInterceptor() : new NoopDeserializedInterceptor());
    }

    private void populateData() {
        IgniteBinary binary = grid(SERVER_NODE_NAME).context().cacheObjects().binary();
        for (String cacheName : CACHE_NAMES) {
            IgniteCache cache = grid(SERVER_NODE_NAME).cache(cacheName);

            for (int i = 0; i < 1000; i++) {
                Value v = new Value(cacheName + i);

                if (binary())
                    cache.put(i, binary.toBinary(v));
                else
                    cache.put(i, v);
            }
        }
    }

    protected abstract boolean binary();

    protected static class Value {
        private final String v;

        public Value(String v) {
            this.v = v;
        }

        public String v() {
            return v;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value value = (Value)o;

            return Objects.equals(v, value.v);
        }

        @Override public int hashCode() {
            return Objects.hash(v);
        }
    }

    private static class NoopDeserializedInterceptor extends CacheInterceptorDeserializeAdapter<Integer, Value> {
        /** {@inheritDoc} */
        @Nullable @Override public Value onGet(Integer key, Value val) {
            return super.onGet(key, val);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Value onBeforePut(Cache.Entry<Integer, Value> entry, Value newVal) {
            return super.onBeforePut(entry, newVal);
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Value> entry) {
            super.onAfterPut(entry);
        }

        /** {@inheritDoc} */
        @Override public @Nullable IgniteBiTuple<Boolean, Value> onBeforeRemove(Cache.Entry<Integer, Value> entry) {
            return super.onBeforeRemove(entry);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Value> entry) {
            super.onAfterRemove(entry);
        }
    }

    private static class NoopBinaryInterceptor extends CacheInterceptorAdapter<KeyCacheObject, BinaryObject> {
        /** {@inheritDoc} */
        @Nullable @Override public BinaryObject onGet(KeyCacheObject key, BinaryObject val) {
            checkValue(val);

            return super.onGet(key, val);
        }

        /** {@inheritDoc} */
        @Nullable @Override
        public BinaryObject onBeforePut(Cache.Entry<KeyCacheObject, BinaryObject> entry, BinaryObject newVal) {
            checkValue(entry.getValue());
            checkValue(newVal);

            return super.onBeforePut(entry, newVal);
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<KeyCacheObject, BinaryObject> entry) {
            checkValue(entry.getValue());

            super.onAfterPut(entry);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable IgniteBiTuple<Boolean, BinaryObject> onBeforeRemove(Cache.Entry<KeyCacheObject, BinaryObject> entry) {
            checkValue(entry.getValue());

            return super.onBeforeRemove(entry);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<KeyCacheObject, BinaryObject> entry) {
            checkValue(entry.getValue());

            super.onAfterRemove(entry);
        }

        private void checkValue(BinaryObject obj) {
            if (obj != null) {
                Object deserVal = obj.deserialize();

                assertTrue(deserVal.getClass().getName(), deserVal instanceof Value);
            }
        }
    }
}
