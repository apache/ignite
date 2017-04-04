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

package org.apache.ignite.configuration;

import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_NEAR_START_SIZE;

/**
 * Client (near) cache configuration.
 * <p>
 * Distributed cache can also be fronted by a Near cache,
 * which is a smaller local cache that stores most recently
 * or most frequently accessed data. Just like with a partitioned cache,
 * the user can control the size of the near cache and its eviction policies.
 */
public class NearCacheConfiguration<K, V> extends MutableConfiguration<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near cache eviction policy. */
    private EvictionPolicy<K, V> nearEvictPlc;

    /** Default near cache start size. */
    private int nearStartSize = DFLT_NEAR_START_SIZE;

    /**
     * Empty constructor.
     */
    public NearCacheConfiguration() {
        // No-op.
    }

    /**
     * Creates near cache configuration copying properties from passed in configuration.
     *
     * @param ccfg Configuration to copy.
     */
    public NearCacheConfiguration(NearCacheConfiguration<K, V> ccfg) {
        super(ccfg);

        nearEvictPlc = ccfg.getNearEvictionPolicy();
        nearStartSize = ccfg.getNearStartSize();
    }

    /**
     * Gets near eviction policy. By default, returns {@code null}
     * which means that evictions are disabled for near cache.
     *
     * @return Near eviction policy.
     * @see CacheConfiguration#getEvictionPolicy()
     * @see CacheConfiguration#isEvictSynchronized()
     */
    public EvictionPolicy<K, V> getNearEvictionPolicy() {
        return nearEvictPlc;
    }

    /**
     * Sets near eviction policy.
     *
     * @param nearEvictPlc Near eviction policy.
     * @return {@code this} for chaining.
     */
    public NearCacheConfiguration<K, V> setNearEvictionPolicy(EvictionPolicy<K, V> nearEvictPlc) {
        this.nearEvictPlc = nearEvictPlc;

        return this;
    }

    /**
     * Gets initial cache size for near cache which will be used to pre-create internal
     * hash table after start. Default value is defined by {@link CacheConfiguration#DFLT_NEAR_START_SIZE}.
     *
     * @return Initial near cache size.
     */
    public int getNearStartSize() {
        return nearStartSize;
    }

    /**
     * Start size for near cache. This property is only used for {@link CacheMode#PARTITIONED} caching mode.
     *
     * @param nearStartSize Start size for near cache.
     * @return {@code this} for chaining.
     */
    public NearCacheConfiguration<K, V> setNearStartSize(int nearStartSize) {
        this.nearStartSize = nearStartSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setTypes(Class<K> keyType, Class<V> valueType) {
        return (CacheConfiguration<K, V>)super.setTypes(keyType, valueType);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setCacheLoaderFactory(Factory<? extends CacheLoader<K, V>> factory) {
        return (CacheConfiguration<K, V>)super.setCacheLoaderFactory(factory);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setCacheWriterFactory(
        Factory<? extends CacheWriter<? super K, ? super V>> factory) {
        return (CacheConfiguration<K, V>)super.setCacheWriterFactory(factory);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends ExpiryPolicy> factory) {
        return (CacheConfiguration<K, V>)super.setExpiryPolicyFactory(factory);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setReadThrough(boolean isReadThrough) {
        return (CacheConfiguration<K, V>)super.setReadThrough(isReadThrough);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setWriteThrough(boolean isWriteThrough) {
        return (CacheConfiguration<K, V>)super.setWriteThrough(isWriteThrough);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setStoreByValue(boolean isStoreByValue) {
        return (CacheConfiguration<K, V>)super.setStoreByValue(isStoreByValue);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setStatisticsEnabled(boolean enabled) {
        return (CacheConfiguration<K, V>)super.setStatisticsEnabled(enabled);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setManagementEnabled(boolean enabled) {
        return (CacheConfiguration<K, V>)super.setManagementEnabled(enabled);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NearCacheConfiguration.class, this, super.toString());
    }
}
