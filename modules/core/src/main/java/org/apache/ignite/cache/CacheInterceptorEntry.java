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

package org.apache.ignite.cache;

import javax.cache.Cache;

/**
 * A cache interceptor map entry.
 *
 * @param <K> The type of key.
 * @param <V> The type of value.
 */
public abstract class CacheInterceptorEntry<K, V> implements Cache.Entry<K, V> {
    /**
     * Each cache update increases partition counter. The same cache updates have on the same value of counter
     * on primary and backup nodes. This value can be useful to communicate with external applications.
     * The value has sense only for entries get by {@link CacheInterceptor#onAfterPut(Cache.Entry)} and
     * {@link CacheInterceptor#onAfterRemove(Cache.Entry)} methods. For entries got by other methods will return
     * {@code 0}.
     *
     * @return Value of counter for this entry.
     */
    public abstract long getPartitionUpdateCounter();
}
