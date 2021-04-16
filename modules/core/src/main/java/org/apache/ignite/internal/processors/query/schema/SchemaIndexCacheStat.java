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

package org.apache.ignite.internal.processors.query.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Class for accumulation of record types and number of indexed records in index tree.
 */
public class SchemaIndexCacheStat {
    /** Indexed types. */
    private final Map<String, QueryTypeDescriptorImpl> types = new HashMap<>();

    /** Number of indexed keys. */
    private int scanned;

    /**
     * Adds statistics from {@code stat} to the current statistics.
     *
     * @param stat Statistics.
     */
    public void accumulate(SchemaIndexCacheStat stat) {
        scanned += stat.scanned;
        types.putAll(stat.types);
    }

    /**
     * Adds type to indexed types.
     *
     * @param type Type.
     */
    public void addType(QueryTypeDescriptorImpl type) {
        types.put(type.name(), type);
    }

    /**
     * Adds to number of scanned keys given {@code scanned}.
     *
     * @param scanned Number of scanned keys during partition processing. Must be positive or zero.
     */
    public void add(int scanned) {
        A.ensure(scanned >= 0, "scanned is negative. Value: " + scanned);

        this.scanned += scanned;
    }

    /**
     * @return Number of scanned keys.
     */
    public int scannedKeys() {
        return scanned;
    }

    /**
     * @return Unmodifiable collection of processed type names.
     */
    public Collection<String> typeNames() {
        return Collections.unmodifiableCollection(types.keySet());
    }

    /**
     * @return Unmodifiable collection of processed types.
     */
    public Collection<QueryTypeDescriptorImpl> types() {
        return Collections.unmodifiableCollection(types.values());
    }

}
