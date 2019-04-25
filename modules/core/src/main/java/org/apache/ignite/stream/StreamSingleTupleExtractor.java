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
 * Stream tuple extractor to convert a message to a single Ignite key-value tuple.
 * <p>
 * Alternatively, {@link StreamMultipleTupleExtractor} can be used in cases where a single message/event may
 * produce more than one tuple.
 * <p>
 * NOTE: This interface supersedes the former {@link StreamTupleExtractor} which is now deprecated.
 *
 * @see StreamMultipleTupleExtractor
 */
public interface StreamSingleTupleExtractor<T, K, V> {
    /**
     * Extracts a key-value tuple from a message.
     *
     * @param msg Message.
     * @return Key-value tuple.
     */
    public Map.Entry<K, V> extract(T msg);
}