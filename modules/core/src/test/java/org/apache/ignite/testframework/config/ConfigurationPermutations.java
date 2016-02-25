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
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigPermutationsAbstractTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;

import static org.apache.ignite.internal.util.lang.GridFunc.asArray;
import static org.apache.ignite.testframework.config.Parameters.booleanParameters;
import static org.apache.ignite.testframework.config.Parameters.complexParameter;
import static org.apache.ignite.testframework.config.Parameters.enumParameters;
import static org.apache.ignite.testframework.config.Parameters.factory;
import static org.apache.ignite.testframework.config.Parameters.objectParameters;
import static org.apache.ignite.testframework.config.Parameters.parameter;

/**
 * Cache configuration permutations.
 */
@SuppressWarnings("serial") public class ConfigurationPermutations {
    /** */
    public static final ConfigurationParameter<Object> EVICTION_PARAM = complexParameter(
        parameter("setEvictionPolicy", factory(FifoEvictionPolicy.class)),
        parameter("setEvictionFilter", factory(NoopEvictionFilter.class))
    );

    /** */
    public static final ConfigurationParameter<Object> CACHE_STORE_PARAM = complexParameter(
        parameter("setCacheStoreFactory", factory(IgniteCacheConfigPermutationsAbstractTest.TestStoreFactory.class)),
        parameter("setReadThrough", true),
        parameter("setWriteThrough", true),
        parameter("setCacheStoreSessionListenerFactories", noopCacheStoreSessionListenerFactory())
    );

    /** */
    public static final ConfigurationParameter<Object> SIMPLE_CACHE_STORE_PARAM = complexParameter(
        parameter("setCacheStoreFactory", factory(IgniteCacheConfigPermutationsAbstractTest.TestStoreFactory.class)),
        parameter("setReadThrough", true),
        parameter("setWriteThrough", true)
    );

    /** */
    public static final ConfigurationParameter<Object> REBALANCING_PARAM = complexParameter(
        parameter("setRebalanceBatchSize", 2028 * 1024),
        parameter("setRebalanceBatchesPrefetchCount", 5L),
        parameter("setRebalanceThreadPoolSize", 5),
        parameter("setRebalanceTimeout", CacheConfiguration.DFLT_REBALANCE_TIMEOUT * 2),
        parameter("setRebalanceDelay", 1000L)
    );

    /** */
    public static final ConfigurationParameter<Object> ONHEAP_TIERED_MEMORY_PARAM =
        parameter("setMemoryMode", CacheMemoryMode.ONHEAP_TIERED);

    /** */
    public static final ConfigurationParameter<Object> OFFHEAP_TIERED_MEMORY_PARAM =
        parameter("setMemoryMode", CacheMemoryMode.OFFHEAP_TIERED);

    /** */
    public static final ConfigurationParameter<Object> OFFHEAP_VALUES_MEMORY_PARAM =
        parameter("setMemoryMode", CacheMemoryMode.OFFHEAP_VALUES);

    /** */
    public static final ConfigurationParameter<Object> OFFHEAP_ENABLED =
        parameter("setOffHeapMaxMemory", 10 * 1024 * 1024L);

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigurationParameter<IgniteConfiguration>[][] BASIC_IGNITE_SET = new ConfigurationParameter[][] {
        objectParameters("setMarshaller", factory(BinaryMarshaller.class), optimizedMarshallerFactory()),
        booleanParameters("setPeerClassLoadingEnabled"),
        objectParameters("setSwapSpaceSpi", factory(GridTestSwapSpaceSpi.class)),
    };

    /** */
    @SuppressWarnings("unchecked")
    public static final ConfigurationParameter<CacheConfiguration>[][] BASIC_SET = new ConfigurationParameter[][] {
        objectParameters("setCacheMode", CacheMode.REPLICATED, CacheMode.PARTITIONED),
        enumParameters("setAtomicityMode", CacheAtomicityMode.class),
        enumParameters("setMemoryMode", CacheMemoryMode.class),
        // Set default parameters.
        objectParameters("setLoadPreviousValue", true),
        objectParameters("setSwapEnabled", true),
        asArray(SIMPLE_CACHE_STORE_PARAM),
        objectParameters("setWriteSynchronizationMode", CacheWriteSynchronizationMode.FULL_SYNC),
        objectParameters("setAtomicWriteOrderMode", CacheAtomicWriteOrderMode.PRIMARY),
        objectParameters("setStartSize", 1024),
    };

    /** */
    @SuppressWarnings("unchecked")
    public static final ConfigurationParameter<CacheConfiguration>[][] DEFAULT_SET = new ConfigurationParameter[][] {
        enumParameters("setCacheMode", CacheMode.class),
        enumParameters("setAtomicityMode", CacheAtomicityMode.class),
        asArray(ONHEAP_TIERED_MEMORY_PARAM,
            complexParameter(ONHEAP_TIERED_MEMORY_PARAM, OFFHEAP_ENABLED),
            complexParameter(OFFHEAP_TIERED_MEMORY_PARAM, OFFHEAP_ENABLED),
            complexParameter(OFFHEAP_VALUES_MEMORY_PARAM, OFFHEAP_ENABLED)
        ),
        booleanParameters("setLoadPreviousValue"),
        booleanParameters("setReadFromBackup"),
        booleanParameters("setStoreKeepBinary"),
        objectParameters("setRebalanceMode", CacheRebalanceMode.SYNC, CacheRebalanceMode.ASYNC),
        booleanParameters("setSwapEnabled"),
        booleanParameters("setCopyOnRead"),
        objectParameters(true, "setNearConfiguration", nearCacheConfigurationFactory()),
        asArray(null,
            complexParameter(
                EVICTION_PARAM,
                CACHE_STORE_PARAM,
                REBALANCING_PARAM,
                parameter("setAffinity", factory(FairAffinityFunction.class)),
                parameter("setInterceptor", factory(NoopInterceptor.class)),
                parameter("setTopologyValidator", factory(NoopTopologyValidator.class)),
                parameter("addCacheEntryListenerConfiguration", factory(EmptyCacheEntryListenerConfiguration.class))
            )
        ),
        // Set default parameters.
        objectParameters("setWriteSynchronizationMode", CacheWriteSynchronizationMode.FULL_SYNC),
        objectParameters("setAtomicWriteOrderMode", CacheAtomicWriteOrderMode.PRIMARY),
        objectParameters("setStartSize", 1024),
    };

    /**
     * Private constructor.
     */
    private ConfigurationPermutations() {
        // No-op.
    }

    /**
     * @return Custom near cache config.
     */
    private static Factory nearCacheConfigurationFactory() {
        return new Factory() {
            @Override public Object create() {
                NearCacheConfiguration cfg = new NearCacheConfiguration<>();

                cfg.setNearEvictionPolicy(new FifoEvictionPolicy());

                return cfg;
            }
        };
    }

    /**
     * @return Noop cache store session listener factory.
     */
    private static Factory noopCacheStoreSessionListenerFactory() {
        return new Factory() {
            @Override public Object create() {
                return new Factory[] {new NoopCacheStoreSessionListenerFactory()};
            }
        };
    }

    /**
     * @return Default matrix of availiable permutations.
     */
    public static ConfigurationParameter<CacheConfiguration>[][] cacheBasicSet() {
        return BASIC_SET;
    }

    /**
     * @return Default matrix of availiable permutations.
     */
    public static ConfigurationParameter<CacheConfiguration>[][] cacheDefaultSet() {
        return DEFAULT_SET;
    }

    /**
     * @return Default matrix of availiable permutations.
     */
    public static ConfigurationParameter<IgniteConfiguration>[][] igniteBasicSet() {
        return BASIC_IGNITE_SET;
    }

    /**
     * @return Marshaller.
     */
    private static Factory<OptimizedMarshaller> optimizedMarshallerFactory() {
        return new Factory<OptimizedMarshaller>() {
            @Override public OptimizedMarshaller create() {
                OptimizedMarshaller marsh = new OptimizedMarshaller(true);

                marsh.setRequireSerializable(false);

                return marsh;
            }
        };
    }

    /**
     *
     */
    public static class NoopEvictionFilter implements EvictionFilter {
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
    public static class NoopInterceptor extends CacheInterceptorAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     *
     */
    public static class NoopCacheStoreSessionListenerFactory implements Factory<NoopCacheStoreSessionListener> {
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
    public static class NoopCacheStoreSessionListener implements CacheStoreSessionListener {
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
    public static class NoopTopologyValidator implements TopologyValidator {
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
    @SuppressWarnings({"serial", "unchecked"})
    public static class EmptyCacheEntryListenerConfiguration extends MutableCacheEntryListenerConfiguration {
        /**
         *
         */
        public EmptyCacheEntryListenerConfiguration() {
            super(new NoopCacheEntryListenerConfiguration());
        }
    }

    /**
     *
     */
    @SuppressWarnings("serial")
    public static class NoopCacheEntryListenerConfiguration implements CacheEntryListenerConfiguration {
        /** {@inheritDoc} */
        @Override public Factory<CacheEntryListener> getCacheEntryListenerFactory() {
            return new Factory<CacheEntryListener>() {
                @Override public CacheEntryListener create() {
                    return new NoopCacheEntryListener();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public boolean isOldValueRequired() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Factory<CacheEntryEventFilter> getCacheEntryEventFilterFactory() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isSynchronous() {
            return false;
        }
    }

    /**
     *
     */
    public static class NoopCacheEntryListener implements CacheEntryCreatedListener {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable iterable) throws CacheEntryListenerException {
            // No-op.
        }
    }
}
