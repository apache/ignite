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

package org.apache.ignite.stream.twitter;

import java.util.Map;

/**
 * Implement this interface to transform from a Tweet JSON to a set of cache entries in the form of a {@link Map}.
 *
 * @param <K> The type of the cache key.
 * @param <V> The type of the cache value.
 */
public interface TweetTransformer<K, V> {
    /**
     * Transformation function.
     *
     * @param tweet The message (Tweet JSON String) received from the Twitter Streaming API.
     * @return Set of cache entries to add to the cache. It could be empty or null if the message should be skipped.
     */
    Map<K, V> apply(String tweet);
}
