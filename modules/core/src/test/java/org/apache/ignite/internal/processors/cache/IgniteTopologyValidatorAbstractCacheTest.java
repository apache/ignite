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

import java.io.Serializable;
import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.transactions.Transaction;

/**
 * Topology validator test.
 */
public abstract class IgniteTopologyValidatorAbstractCacheTest extends IgniteCacheAbstractTest implements Serializable {
    /** key-value used at test. */
    protected static String KEY_VAL = "1";

    /** cache name 1. */
    protected static String CACHE_NAME_1 = "cache1";

    /** cache name 2. */
    protected static String CACHE_NAME_2 = "cache2";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        CacheConfiguration cCfg0 = cacheConfiguration(gridName);

        CacheConfiguration cCfg1 = cacheConfiguration(gridName);
        cCfg1.setName(CACHE_NAME_1);

        CacheConfiguration cCfg2 = cacheConfiguration(gridName);
        cCfg2.setName(CACHE_NAME_2);

        iCfg.setCacheConfiguration(cCfg0, cCfg1, cCfg2);

        for (CacheConfiguration cCfg : iCfg.getCacheConfiguration()) {
            if (cCfg.getName() != null)
                if (cCfg.getName().equals(CACHE_NAME_1))
                    cCfg.setTopologyValidator(new TopologyValidator() {
                        @Override public boolean validate(Collection<ClusterNode> nodes) {
                            return nodes.size() == 2;
                        }
                    });
                else if (cCfg.getName().equals(CACHE_NAME_2))
                    cCfg.setTopologyValidator(new TopologyValidator() {
                        @Override public boolean validate(Collection<ClusterNode> nodes) {
                            return nodes.size() >= 2;
                        }
                    });
        }

        return iCfg;
    }

    /**
     * Puts when topology is invalid.
     *
     * @param cacheName cache name.
     */
    protected void putInvalid(String cacheName) {
        try {
            grid(0).cache(cacheName).put(KEY_VAL, KEY_VAL);

            assert false : "topology validation broken";
        }
        catch (CacheException ex) {
            assert ex.getCause() instanceof IgniteCheckedException &&
                ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * Puts when topology is valid.
     *
     * @param cacheName cache name.
     */
    protected void putValid(String cacheName) {
        try {
            grid(0).cache(cacheName).put(KEY_VAL, KEY_VAL);

            assert grid(0).cache(cacheName).get(KEY_VAL).equals(KEY_VAL);
        }
        catch (CacheException ignored) {
            assert false : "topology validation broken";
        }
    }

    /**
     * Gets when topology is invalid.
     *
     * @param cacheName cache name.
     */
    protected void getInvalid(String cacheName) {
        try {
            assert grid(0).cache(cacheName).get(KEY_VAL).equals(KEY_VAL);
        }
        catch (CacheException ignored) {
            assert false : "topology validation broken";
        }
    }

    /**
     * Remove when topology is invalid.
     *
     * @param cacheName cache name.
     */
    protected void removeInvalid(String cacheName) {
        try {
            grid(0).cache(cacheName).remove(KEY_VAL);

            assert false : "topology validation broken";
        }
        catch (CacheException ex) {
            assert ex.getCause() instanceof IgniteCheckedException &&
                ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * Commits with error.
     *
     * @param tx transaction.
     */
    protected void commitFailed(Transaction tx) {
        try {
            tx.commit();
        }
        catch (IgniteException ex) {
            assert ex.getCause() instanceof IgniteCheckedException &&
                ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * Removes key-value.
     *
     * @param cacheName cache name.
     */
    public void remove(String cacheName) {
        assert grid(0).cache(cacheName).get(KEY_VAL) != null;

        grid(0).cache(cacheName).remove(KEY_VAL);
    }

    /**
     * Asserts that cache doesn't contains key.
     *
     * @param cacheName cache name.
     */
    public void assertEmpty(String cacheName) {
        assert grid(0).cache(cacheName).get(KEY_VAL) == null;
    }

    /** topology validator test. */
    public void testTopologyValidator() throws Exception {

        putValid(null);
        remove(null);

        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putInvalid(CACHE_NAME_2);
        removeInvalid(CACHE_NAME_2);

        startGrid(1);

        putValid(null);
        remove(null);

        putValid(CACHE_NAME_1);

        putValid(CACHE_NAME_2);
        remove(CACHE_NAME_2);

        startGrid(2);

        putValid(null);
        remove(null);

        getInvalid(CACHE_NAME_1);
        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putValid(CACHE_NAME_2);
        remove(CACHE_NAME_2);
    }
}
