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

package org.apache.ignite.calcite;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteExperimental;

/** Context for extracting value of a virtual column. */
// TODO: IGNITE-28223 Может еще немного подумать на счет API
@IgniteExperimental
public interface VirtualColumnValueExtractorContext {
    /** Returns cache ID. */
    int cacheId();

    /** Returns cache name. */
    String cacheName();

    /** Returns partition ID. */
    int partition();

    /**
     * Returns source for getting value of virtual column.
     *
     * @param keyOrValue {@code true} if a cache key (primary key) is needed, {@code false} if a cache value is needed.
     * @param keepBinary {@code true} if returned as {@link BinaryObject}. If key or value is not composite, it may
     *      return not as {@link BinaryObject}, but as a simple type.
     */
    Object source(boolean keyOrValue, boolean keepBinary);
}
