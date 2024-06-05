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

package org.apache.ignite.dump;

import java.util.Iterator;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Consumer of a cache dump.
 * This consumer will receive all {@link DumpEntry} stored in cache dump during {@code IgniteDumpReader} application invocation.
 * The lifecycle of the consumer is the following:
 * <ul>
 *     <li>Start of the consumer {@link #start()}.</li>
 *     <li>Stop of the consumer {@link #stop()}.</li>
 * </ul>
 *
 */
@IgniteExperimental
public interface DumpConsumer {
    /**
     * Starts the consumer.
     */
    void start();

    /**
     * Handles type mappings.
     * @param mappings Mappings iterator.
     */
    void onMappings(Iterator<TypeMapping> mappings);

    /**
     * Handles binary types.
     * @param types Binary types iterator.
     */
    void onTypes(Iterator<BinaryType> types);

    /**
     * Handles cache configs.
     * Note, there can be several copies of cache config in the dump.
     * This can happen if dump contains data from several nodes.
     * @param caches Stored cache data.
     */
    void onCacheConfigs(Iterator<StoredCacheData> caches);

    /**
     * Handles cache data.
     * This method can be invoked by several threads concurrently.
     * Note, there can be several copies of group partition in the dump.
     * This can happen if dump contains data from several nodes.
     * In this case callback will be invoked several time for the same pair of [grp, part] values.
     *
     * @param grp Group id.
     * @param part Partition.
     * @param data Cache data iterator.
     * @see DumpReaderConfiguration#threadCount()
     */
    void onPartition(int grp, int part, Iterator<DumpEntry> data);

    /**
     * Stops the consumer.
     * This method can be invoked only after {@link #start()}.
     */
    void stop();
}
