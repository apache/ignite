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

package org.gridgain.grid.cache.spring;

import org.springframework.cache.annotation.*;

/**
 * Test service.
 */
public class GridSpringDynamicCacheTestService {
    /**
     * @param key Key.
     * @return Value.
     */
    @Cacheable({"testCache1", "testCache2"})
    public String cacheable(Integer key) {
        assert key != null;

        return "value" + key;
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @CachePut({"testCache1", "testCache2"})
    public String cachePut(Integer key) {
        assert key != null;

        return "value" + key;
    }

    /**
     * @param key Key.
     */
    @CacheEvict("testCache1")
    public void cacheEvict(Integer key) {
        // No-op.
    }

    /**
     */
    @CacheEvict(value = "testCache1", allEntries = true)
    public void cacheEvictAll() {
        // No-op.
    }
}
