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

package org.apache.ignite.internal.processors.rest.request;

import java.util.Map;
import org.apache.ignite.internal.processors.cache.CacheConfigurationOverride;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache command request descriptor.
 */
public class GridRestCacheRequest extends GridRestRequest {
    /** Cache name. */
    private String cacheName;

    /** Template name. */
    private String templateName;

    /** Key. */
    private Object key;

    /** Value (expected value for CAS). */
    private Object val;

    /** New value for CAS. */
    private Object val2;

    /** Keys and values for put all, get all, remove all operations. */
    private Map<Object, Object> vals;

    /** Cache configuration parameters. */
    private CacheConfigurationOverride cfg;

    /** Bit map of cache flags to be enabled on cache projection. */
    private int cacheFlags;

    /** Expiration time. */
    private Long ttl;

    /**
     * @return Cache name, or {@code null} if not set.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest cacheName(String cacheName) {
        this.cacheName = cacheName;

        return this;
    }

    /**
     * @return Template name, or {@code null} if not set.
     */
    public String templateName() {
        return templateName;
    }

    /**
     * @param templateName Template name.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest templateName(String templateName) {
        this.templateName = templateName;

        return this;
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @param key Key.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest key(Object key) {
        this.key = key;

        return this;
    }

    /**
     * @return Value 1.
     */
    public Object value() {
        return val;
    }

    /**
     * @param val Value 1.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest value(Object val) {
        this.val = val;

        return this;
    }

    /**
     * @return Value 2.
     */
    public Object value2() {
        return val2;
    }

    /**
     * @param val2 Value 2.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest value2(Object val2) {
        this.val2 = val2;

        return this;
    }

    /**
     * @return Keys and values for put all, get all, remove all operations.
     */
    public Map<Object, Object> values() {
        return vals;
    }

    /**
     * @param vals Keys and values for put all, get all, remove all operations.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest values(Map<Object, Object> vals) {
        this.vals = vals;

        return this;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfigurationOverride configuration() {
        return cfg;
    }

    /**
     * @param cfg Cache configuration.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest configuration(CacheConfigurationOverride cfg) {
        this.cfg = cfg;

        return this;
    }

    /**
     * @param cacheFlags Bit representation of cache flags.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest cacheFlags(int cacheFlags) {
        this.cacheFlags = cacheFlags;

        return this;
    }

    /**
     * @return Bit representation of cache flags.
     */
    public int cacheFlags() {
        return cacheFlags;
    }

    /**
     * @return Expiration time.
     */
    public Long ttl() {
        return ttl;
    }

    /**
     * @param ttl Expiration time.
     * @return This GridRestCacheRequest for chaining.
     */
    public GridRestCacheRequest ttl(Long ttl) {
        this.ttl = ttl;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestCacheRequest.class, this, super.toString());
    }
}
