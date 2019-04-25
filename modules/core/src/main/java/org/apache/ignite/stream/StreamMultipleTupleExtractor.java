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

package org.apache.ignite.stream;

import java.util.Map;

/**
 * Stream tuple extractor to convert a single message to zero, one or many tuples.
 * <p>
 * For cases where cardinality will always be 1:1 (or 0:1), you may consider {@link StreamTupleExtractor}.
 *
 * @see StreamTupleExtractor
 */
public interface StreamMultipleTupleExtractor<T, K, V> {

    /**
     * Extracts a set of key-values from a message.
     *
     * @param msg Message.
     * @return Map containing resulting tuples.
     */
    public Map<K, V> extract(T msg);
}