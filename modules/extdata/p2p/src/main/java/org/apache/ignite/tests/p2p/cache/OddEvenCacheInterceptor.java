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

package org.apache.ignite.tests.p2p.cache;

import javax.cache.Cache;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.lang.Math.abs;

/**
 * This cache interceptor checks all input entries in order to all are equivalent by parity of keys to values.
 * If a value does not equal by parity with the key, the interceptor increment the value.
 */
public class OddEvenCacheInterceptor implements CacheInterceptor<Integer, Integer> {
    /** {@inheritDoc} */
    @Override public Integer onGet(Integer key, Integer val) {
        assert abs(key % 2) == abs(val % 2) : "Incorrect entry was put to cache: [key=" + key + ", value=" + val + ']';
        return val;
    }

    /** {@inheritDoc} */
    @Override public Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
        return abs(entry.getKey() % 2) == abs(newVal % 2) ? newVal : newVal + 1;
    }

    /** {@inheritDoc} */
    @Override public void onAfterPut(Cache.Entry<Integer, Integer> entry) {
        assert abs(entry.getKey() % 2) == abs(entry.getValue() % 2) : "Incorrect entry was put to cache: " + entry;
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<Boolean, Integer> onBeforeRemove(Cache.Entry<Integer, Integer> entry) {
        assert abs(entry.getKey() % 2) == abs(entry.getValue() % 2) : "Incorrect entry was put to cache: " + entry;

        return new IgniteBiTuple<>(false, entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void onAfterRemove(Cache.Entry<Integer, Integer> entry) {
        assert abs(entry.getKey() % 2) == abs(entry.getValue() % 2) : "Incorrect entry was put to cache: " + entry;
    }
}
