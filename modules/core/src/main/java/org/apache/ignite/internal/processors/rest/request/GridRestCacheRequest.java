/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Template name, or {@code null} if not set.
     */
    public String templateName() {
        return templateName;
    }

    /**
     * @param templateName Template name.
     */
    public void templateName(String templateName) {
        this.templateName = templateName;
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(Object key) {
        this.key = key;
    }

    /**
     * @return Value 1.
     */
    public Object value() {
        return val;
    }

    /**
     * @param val Value 1.
     */
    public void value(Object val) {
        this.val = val;
    }

    /**
     * @return Value 2.
     */
    public Object value2() {
        return val2;
    }

    /**
     * @param val2 Value 2.
     */
    public void value2(Object val2) {
        this.val2 = val2;
    }

    /**
     * @return Keys and values for put all, get all, remove all operations.
     */
    public Map<Object, Object> values() {
        return vals;
    }

    /**
     * @param vals Keys and values for put all, get all, remove all operations.
     */
    public void values(Map<Object, Object> vals) {
        this.vals = vals;
    }


    /**
     * @return Cache configuration.
     */
    public CacheConfigurationOverride configuration() {
        return cfg;
    }

    /**
     * @param cfg Cache configuration.
     */
    public void configuration(CacheConfigurationOverride cfg) {
        this.cfg = cfg;
    }

    /**
     * @param cacheFlags Bit representation of cache flags.
     */
    public void cacheFlags(int cacheFlags) {
        this.cacheFlags = cacheFlags;
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
     */
    public void ttl(Long ttl) {
        this.ttl = ttl;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestCacheRequest.class, this, super.toString());
    }
}
