/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksDB;

/**
 * Utilities for converting partition IDs and index names into Column Family names and vice versa.
 */
class ColumnFamilyUtils {
    /**
     * Name of the meta column family matches default columns family, meaning that it always exist when new table is created.
     */
    static final String META_CF_NAME = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);

    /**
     * Name of the Column Family that stores partition data.
     */
    static final String PARTITION_CF_NAME = "cf-part";

    /**
     * Prefix for SQL indexes column family names.
     */
    private static final String CF_SORTED_INDEX_PREFIX = "cf-sorted-idx-";

    /**
     * Utility enum to describe a type of the column family - meta, partition or index.
     */
    enum ColumnFamilyType {
        META, PARTITION, SORTED_INDEX, UNKNOWN
    }

    /**
     * Creates column family name by index name.
     *
     * @param indexName Index name.
     * @return Column family name.
     * 
     * @see #sortedIndexName
     */
    static String sortedIndexCfName(String indexName) {
        return CF_SORTED_INDEX_PREFIX + indexName;
    }

    /**
     * Creates a Sorted Index name from the given Column Family name.
     *
     * @param cfName Column Family name.
     * @return Sorted Index name.
     *
     * @see #sortedIndexCfName
     */
    static String sortedIndexName(String cfName) {
        return cfName.substring(CF_SORTED_INDEX_PREFIX.length());
    }

    /**
     * Determines column family type by its name.
     *
     * @param cfName Column family name.
     * @return Column family type.
     */
    static ColumnFamilyType columnFamilyType(String cfName) {
        if (META_CF_NAME.equals(cfName)) {
            return ColumnFamilyType.META;
        }

        if (PARTITION_CF_NAME.equals(cfName)) {
            return ColumnFamilyType.PARTITION;
        }

        if (cfName.startsWith(CF_SORTED_INDEX_PREFIX)) {
            return ColumnFamilyType.SORTED_INDEX;
        }

        return ColumnFamilyType.UNKNOWN;
    }
}
