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

package org.apache.ignite.cache.spring;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

/**
 * Test service.
 */
public class GridSpringCacheTestService {
    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /**
     * @return How many times service was called.
     */
    public int called() {
        return cnt.get();
    }

    /**
     * Resets service.
     */
    public void reset() {
        cnt.set(0);
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @Cacheable("testCache")
    public String simpleKey(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return "value" + key;
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @Cacheable("testCache")
    public String simpleKeyNullValue(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return null;
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     * @return Value.
     */
    @Cacheable("testCache")
    public String complexKey(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        cnt.incrementAndGet();

        return "value" + p1 + p2;
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     * @return Value.
     */
    @Cacheable("testCache")
    public String complexKeyNullValue(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        cnt.incrementAndGet();

        return null;
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @CachePut("testCache")
    public String simpleKeyPut(Integer key) {
        assert key != null;

        int cnt0 = cnt.incrementAndGet();

        return "value" + key + (cnt0 % 2 == 0 ? "even" : "odd");
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @CachePut("testCache")
    public String simpleKeyPutNullValue(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return null;
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     * @return Value.
     */
    @CachePut("testCache")
    public String complexKeyPut(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        int cnt0 = cnt.incrementAndGet();

        return "value" + p1 + p2 + (cnt0 % 2 == 0 ? "even" : "odd");
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     * @return Value.
     */
    @CachePut("testCache")
    public String complexKeyPutNullValue(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        cnt.incrementAndGet();

        return null;
    }

    /**
     * @param key Key.
     */
    @CacheEvict("testCache")
    public void simpleKeyEvict(Integer key) {
        // No-op.
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     */
    @CacheEvict("testCache")
    public void complexKeyEvict(Integer p1, String p2) {
        // No-op.
    }

    /**
     */
    @CacheEvict(value = "testCache", allEntries = true)
    public void evictAll() {
        // No-op.
    }
}
