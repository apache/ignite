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

package org.apache.ignite.internal.processors.dr;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public interface GridVersionedEntry<K, V> extends Map.Entry<K, V> {
    /**
     * Gets entry's key.
     *
     * @return Entry's key.
     */
    public K key();

    /**
     * Gets entry's value.
     *
     * @return Entry's value.
     */
    @Nullable public V value();

    /**
     * Gets entry's TTL.
     *
     * @return Entry's TTL.
     */
    public long ttl();

    /**
     * Gets entry's expire time.
     *
     * @return Entry's expire time.
     */
    public long expireTime();

    /**
     * @return Version.
     */
    public GridCacheVersion version();

    /**
     * Perform internal marshal of this entry before it will be serialized.
     *
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void marshal(IgniteMarshaller marsh) throws IgniteCheckedException;

    /**
     * Perform internal unmarshal of this entry. It must be performed after entry is deserialized and before
     * its restored key/value are needed.
     *
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshal(IgniteMarshaller marsh) throws IgniteCheckedException;
}
