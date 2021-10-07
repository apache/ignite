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

package org.apache.ignite.configuration.schemas.store;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.Min;
import org.apache.ignite.configuration.validation.OneOf;

/**
 * Configuration schema for data region. Currently it represents configuration for rocksdb storage engine only.
 */
@Config
public class DataRegionConfigurationSchema {
    /** Type of the RocksDB data region. */
    public static final String ROCKSDB_DATA_REGION_TYPE = "rocksdb";

    /** Cache type for the RocksDB LRU cache. */
    public static final String ROCKSDB_LRU_CACHE = "lru";

    /** Cache type for the RocksDB LRU cache. */
    public static final String ROCKSDB_CLOCK_CACHE = "clock";

    /** Type for the future polymorphic configuration schemas. */
    @Immutable
    @OneOf(ROCKSDB_DATA_REGION_TYPE)
    @Value(hasDefault = true)
    public String type = ROCKSDB_DATA_REGION_TYPE;

    /** Size of the rocksdb offheap cache. */
    @Value(hasDefault = true)
    public long size = 256 * 1024 * 1024;

    /** Size of rocksdb write buffer. */
    @Value(hasDefault = true)
    @Min(1)
    public long writeBufferSize = 64 * 1024 * 1024;

    /** Cache type - only {@code LRU} is supported at the moment. {@code Clock} implementation has known bugs. */
    @OneOf({ROCKSDB_LRU_CACHE})
    @Value(hasDefault = true)
    public String cache = ROCKSDB_LRU_CACHE;

    /** The cache is sharded to 2^numShardBits shards, by hash of the key. */
    @Min(-1)
    @Value(hasDefault = true)
    public int numShardBits = -1;
}
