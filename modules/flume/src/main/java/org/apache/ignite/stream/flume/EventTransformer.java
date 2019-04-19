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

package org.apache.ignite.stream.flume;

import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Flume event transformer to convert a list of Flume Events to cache entries.
 */
public interface EventTransformer<Event, K, V> {

    /**
     * Transforms a list of Flume Events to cache entries.
     *
     * @param events List of Flume events to transform.
     * @return Cache entries to be written into the grid.
     */
    @Nullable Map<K, V> transform(List<Event> events);
}
