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

package org.apache.ignite.testframework.configvariations;

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
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;

import static org.apache.ignite.internal.util.lang.GridFunc.asArray;

/**
 * Cache configuration variations.
 */
@SuppressWarnings("serial")
public class ConfigVariations {
    /** */
    private static final ConfigParameter<Object> EVICTION_PARAM = Parameters.complexParameter(
        Parameters.parameter("setEvictionPolicy", Parameters.factory(FifoEvictionPolicy.class)),
        Parameters.parameter("setEvictionFilter", Parameters.factory(NoopEvictionFilter.class))
    );

    /** */
    private static final ConfigParameter<Object> CACHE_STORE_PARAM = Parameters.complexParameter(
        Parameters.parameter("setCacheStoreFactory", Parameters.factory(MapCacheStoreStrategy.MapStoreFactory.class)),
        Parameters.parameter("setReadThrough", true),
        Parameters.parameter("setWriteThrough", true),
        Parameters.parameter("setCacheStoreSessionListenerFactories", noopCacheStoreSessionListenerFactory())
    );

    /** */
    private static final ConfigParameter<Object> SIMPLE_CACHE_STORE_PARAM = Parameters.complexParameter(
        Parameters.parameter("setCacheStoreFactory", Parameters.factory(MapCacheStoreStrategy.MapStoreFactory.class)),
        Parameters.parameter("setReadThrough", true),
        Parameters.parameter("setWriteThrough", true)
    );

    /** */
    private static final ConfigParameter<Object> REBALANCING_PARAM = Parameters.complexParameter(
        Parameters.parameter("setRebalanceBatchSize", 2028 * 1024),
        Parameters.parameter("setRebalanceBatchesPrefetchCount", 5L),
        Parameters.parameter("setRebalanceThreadPoolSize", 5),
        Parameters.parameter("setRebalanceTimeout", CacheConfiguration.DFLT_REBALANCE_TIMEOUT * 2),
        Parameters.parameter("setRebalanceDelay", 1000L)
    );

    /** */
    private static final ConfigParameter<Object> ONHEAP_TIERED_MEMORY_PARAM =
        Parameters.parameter("setMemoryMode", CacheMemoryMode.ONHEAP_TIERED);

    /** */
    private static final ConfigParameter<Object> OFFHEAP_TIERED_MEMORY_PARAM =
        Parameters.parameter("setMemoryMode", CacheMemoryMode.OFFHEAP_TIERED);

    /** */
    private static final ConfigParameter<Object> OFFHEAP_VALUES_MEMORY_PARAM =
        Parameters.parameter("setMemoryMode", CacheMemoryMode.OFFHEAP_VALUES);

    /** */
    private static final ConfigParameter<Object> OFFHEAP_ENABLED =
        Parameters.parameter("setOffHeapMaxMemory", 10 * 1024 * 1024L);

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<IgniteConfiguration>[][] BASIC_IGNITE_SET = new ConfigParameter[][] {
        Parameters.objectParameters("setMarshaller", Parameters.factory(BinaryMarshaller.class), optimizedMarshallerFactory()),
        Parameters.booleanParameters("setPeerClassLoadingEnabled"),
        Parameters.objectParameters("setSwapSpaceSpi", Parameters.factory(GridTestSwapSpaceSpi.class)),
    };

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<CacheConfiguration>[][] BASIC_CACHE_SET = new ConfigParameter[][] {
        Parameters.objectParameters("setCacheMode", CacheMode.REPLICATED, CacheMode.PARTITIONED),
        Parameters.enumParameters("setAtomicityMode", CacheAtomicityMode.class),
        Parameters.enumParameters("setMemoryMode", CacheMemoryMode.class),
        // Set default parameters.
        Parameters.objectParameters("setLoadPreviousValue", true),
        Parameters.objectParameters("setSwapEnabled", true),
        asArray(SIMPLE_CACHE_STORE_PARAM),
        Parameters.objectParameters("setWriteSynchronizationMode", CacheWriteSynchronizationMode.FULL_SYNC),
        Parameters.objectParameters("setAtomicWriteOrderMode", CacheAtomicWriteOrderMode.PRIMARY),
        Parameters.objectParameters("setStartSize", 1024),
    };

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigParameter<CacheConfiguration>[][] FULL_CACHE_SET = new ConfigParameter[][] {
        Parameters.enumParameters("setCacheMode", CacheMode.class),
        Parameters.enumParameters("setAtomicityMode", CacheAtomicityMode.class),
        asArray(ONHEAP_TIERED_MEMORY_PARAM,
            Parameters.complexParameter(ONHEAP_TIERED_MEMORY_PARAM, OFFHEAP_ENABLED),
            Parameters.complexParameter(OFFHEAP_TIERED_MEMORY_PARAM, OFFHEAP_ENABLED),
            Parameters.complexParameter(OFFHEAP_VALUES_MEMORY_PARAM, OFFHEAP_ENABLED)
        ),
        Parameters.booleanParameters("setLoadPreviousValue"),
        Parameters.booleanParameters("setReadFromBackup"),
        Parameters.booleanParameters("setStoreKeepBinary"),
        Parameters.objectParameters("setRebalanceMode", CacheRebalanceMode.SYNC, CacheRebalanceMode.ASYNC),
        Parameters.booleanParameters("setSwapEnabled"),
        Parameters.booleanParameters("setCopyOnRead"),
        Parameters.objectParameters(true, "setNearConfiguration", nearCacheConfigurationFactory()),
        asArray(null,
            Parameters.complexParameter(
                EVICTION_PARAM,
                CACHE_STORE_PARAM,
                REBALANCING_PARAM,
                Parameters.parameter("setAffinity", Parameters.factory(FairAffinityFunction.class)),
                Parameters.parameter("setInterceptor", Parameters.factory(NoopInterceptor.class)),
                Parameters.parameter("setTopologyValidator", Parameters.factory(NoopTopologyValidator.class)),
                Parameters.parameter("addCacheEntryListenerConfiguration", Parameters.factory(EmptyCacheEntryListenerConfiguration.class))
            )
        ),
        // Set default parameters.
        Parameters.objectParameters("setWriteSynchronizationMode", CacheWriteSynchronizationMode.FULL_SYNC),
        Parameters.objectParameters("setAtomicWriteOrderMode", CacheAtomicWriteOrderMode.PRIMARY),
        Parameters.objectParameters("setStartSize", 1024),
    };

    /**
     * Private constructor.
     */
    private ConfigVariations() {
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
     * @return Default matrix of availiable variations.
     */
    public static ConfigParameter<CacheConfiguration>[][] cacheBasicSet() {
        return BASIC_CACHE_SET;
    }

    /**
     * @return Full matrix of availiable variations.
     */
    public static ConfigParameter<CacheConfiguration>[][] cacheFullSet() {
        return FULL_CACHE_SET;
    }

    /**
     * @return Default matrix of availiable variations.
     */
    public static ConfigParameter<IgniteConfiguration>[][] igniteBasicSet() {
        return BASIC_IGNITE_SET;
    }

    /**
     * @return Marshaller.
     */
    public static Factory<OptimizedMarshaller> optimizedMarshallerFactory() {
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
