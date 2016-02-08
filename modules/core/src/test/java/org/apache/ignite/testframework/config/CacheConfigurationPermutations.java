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

package org.apache.ignite.testframework.config;

import java.util.Collection;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.cache.CacheAbstractNewSelfTest;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.config.params.Parameters.booleanParameters;
import static org.apache.ignite.testframework.config.params.Parameters.complexParameter;
import static org.apache.ignite.testframework.config.params.Parameters.enumParameters;
import static org.apache.ignite.testframework.config.params.Parameters.objectParameters;
import static org.apache.ignite.testframework.config.params.Parameters.parameter;

/**
 * Cache configuration permutations.
 */
public class CacheConfigurationPermutations {
    /** */
    public static final ConfigurationParameter<Object> EVICTION_PARAM = complexParameter(
        parameter("setEvictionPolicy", new FifoEvictionPolicy<>()),
        parameter("setEvictionFilter", new NoopEvictionFilter()));

    /** */
    public static final ConfigurationParameter<Object> CACHE_STORE_PARAM = complexParameter(
        parameter("setCacheStoreFactory", new CacheAbstractNewSelfTest.TestStoreFactory()),
        parameter("setCacheStoreSessionListenerFactories", new NoopCacheStoreSessionListenerFactory()));

    /** */
    public static final ConfigurationParameter<Object> REBALANCING_PARAM = complexParameter(parameter("setRebalanceBatchSize", 1024),
        parameter("setRebalanceBatchesPrefetchCount", 5),
        parameter("setRebalanceThreadPoolSize", 5),
        parameter("setRebalanceTimeout", CacheConfiguration.DFLT_REBALANCE_TIMEOUT * 2),
        parameter("setRebalanceDelay", 1000),
        parameter("setRebalanceThrottle", 100));

    /** */
    public static final NearCacheConfiguration NEAR_CACHE_CONFIGURATION = new NearCacheConfiguration();

    /** */
    @SuppressWarnings("unchecked")
    public static final ConfigurationParameter<CacheConfiguration>[][] DEFAULT_SET = new ConfigurationParameter[][] {
        enumParameters("setCacheMode", CacheMode.class),
        enumParameters("setAtomicityMode", CacheAtomicityMode.class),
        enumParameters("setMemoryMode", CacheMemoryMode.class),
        booleanParameters("setLoadPreviousValue"), // TODO add check in tests.
        booleanParameters("setReadFromBackup"), // TODO: add check in tests (disable for tests with localPeek)
        booleanParameters("setStoreKeepBinary"),
        objectParameters("setRebalanceMode", CacheRebalanceMode.SYNC, CacheRebalanceMode.ASYNC),
        booleanParameters("setSwapEnabled"),
        booleanParameters("setCopyOnRead"),
        // TODO uncomment.
//        objectParameters(true, "setNearConfiguration", NEAR_CACHE_CONFIGURATION)
//        asArray(null, complexParameter(EVICTION_PARAM, CACHE_STORE_PARAM, REBALANCING_PARAM,
//            parameter("setAffinity", new FairAffinityFunction()),
//            parameter("setOffHeapMaxMemory", 10 * 1024 * 1024),
//            parameter("setInterceptor", new NoopInterceptor()),
//            parameter("setTopologyValidator", new NoopTopologyValidator()),
//            parameter("addCacheEntryListenerConfiguration", new EmptyCacheEntryListenerConfiguration())
//        )),

        // Set default parameters (TODO make it in builder).
//        objectParameters("setWriteSynchronizationMode", CacheWriteSynchronizationMode.FULL_SYNC), // One value.
//        objectParameters("setAtomicWriteOrderMode", CacheAtomicWriteOrderMode.PRIMARY), // One value.

//        objectParameters("setBackups", 0, 1, 2),// TODO set depending to nodes count.

        // TODO add test for indexes.
//        objectParameters("setIndexedTypes"),// TODO index enabled
//        booleanParameters("setSnapshotableIndex"),// TODO index enabled
    };

    static {
        //noinspection unchecked
        NEAR_CACHE_CONFIGURATION.setNearEvictionPolicy(new FifoEvictionPolicy());
    }

    /**
     * Private constructor.
     */
    private CacheConfigurationPermutations() {
        // No-op.
    }

    /**
     * @return Default matrix of availiable permutations.
     */
    public static ConfigurationParameter<CacheConfiguration>[][] defaultSet() {
        return DEFAULT_SET;
    }

    /**
     *
     */
    private static class NoopEvictionFilter implements EvictionFilter {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public boolean evictAllowed(Cache.Entry entry) {
            return true;
        }
    }

    /**
     *
     */
    private static class NoopInterceptor implements CacheInterceptor {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, @Nullable Object val) {
            return null; // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            return null; // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry entry) {
            // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple onBeforeRemove(Cache.Entry entry) {
            return null; // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry entry) {
            // TODO: CODE: implement.
        }
    }

    /**
     *
     */
    private static class NoopCacheStoreSessionListenerFactory implements Factory<NoopCacheStoreSessionListener> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public NoopCacheStoreSessionListener create() {
            return new NoopCacheStoreSessionListener();
        }
    }

    /**
     *
     */
    private static class NoopCacheStoreSessionListener implements CacheStoreSessionListener {
        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            // No-op.
        }
    }

    /**
     *
     */
    private static class NoopTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return true;
        }
    }

    /**
     *
     */
    private static class EmptyCacheEntryListenerConfiguration implements CacheEntryListenerConfiguration {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Factory<CacheEntryListener> getCacheEntryListenerFactory() {
            return null; // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Override public boolean isOldValueRequired() {
            return false; // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Override public Factory<CacheEntryEventFilter> getCacheEntryEventFilterFactory() {
            return null; // TODO: CODE: implement.
        }

        /** {@inheritDoc} */
        @Override public boolean isSynchronous() {
            return false; // TODO: CODE: implement.
        }
    }
}
