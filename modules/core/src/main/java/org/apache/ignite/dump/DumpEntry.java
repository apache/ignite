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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Single cache entry from dump.
 *
 * @see Dump#iterator(String, int, int, Set)
 * @see DumpConsumer#onPartition(int, int, Iterator)
 * @see org.apache.ignite.IgniteSnapshot#createDump(String, Collection)
 */
@IgniteExperimental
public interface DumpEntry {
    /** @return Cache id. */
    public int cacheId();

    /** @return Expiration time. */
    public long expireTime();

    /**
     * @return Version of the entry.
     */
    public CacheEntryVersion version();

    /** @return Key. */
    public Object key();

    /** @return Value. */
    public Object value();
}
