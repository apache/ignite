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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.lang.IgniteBiTuple;

/** A proxy, which stores given key+value pair to a cache. */
public interface BulkLoadCacheWriter extends AutoCloseable {
    /**
     * Writes given entry to the cache maintaining the update counter.
     *
     * @param entry Entry to store to the cache.
     */
    void accept(IgniteBiTuple<?, ?> entry);

    /**
     * Closes the cache writer releasing underlying resources.
     */
    @Override void close();

    /**
     * Returns number of entry updates made by the writer.
     *
     * @return The number of cache entry updates.
     */
    long updateCnt();
}
