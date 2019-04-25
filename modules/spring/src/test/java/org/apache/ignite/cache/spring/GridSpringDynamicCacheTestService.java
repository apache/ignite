/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.spring;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

/**
 * Test service.
 */
public class GridSpringDynamicCacheTestService {
    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /**
     * @param key Key.
     * @return Value.
     */
    @Cacheable("dynamicCache")
    public String cacheable(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return "value" + key;
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @Cacheable(value = "dynamicCache", sync = true)
    public String cacheableSync(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return "value" + key;
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @CachePut("dynamicCache")
    public String cachePut(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return "value" + key;
    }

    /**
     * @param key Key.
     */
    @CacheEvict("dynamicCache")
    public void cacheEvict(Integer key) {
        cnt.incrementAndGet();
    }

    /**
     */
    @CacheEvict(value = "dynamicCache", allEntries = true)
    public void cacheEvictAll() {
        cnt.incrementAndGet();
    }

    /**
     * @return Calls count.
     */
    public int called() {
        return cnt.get();
    }

    /**
     */
    public void reset() {
        cnt.set(0);
    }
}
