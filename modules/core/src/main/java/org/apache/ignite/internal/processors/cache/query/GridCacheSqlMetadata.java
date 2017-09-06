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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Metadata for Ignite cache.
 * <p>
 * Metadata describes objects stored in the cache and
 * can be used to gather information about what can
 * be queried using Ignite cache queries feature.
 */
public interface GridCacheSqlMetadata extends Externalizable {
    /**
     * Cache name.
     *
     * @return Cache name.
     */
    public String cacheName();

    /**
     * Gets the collection of types stored in cache.
     * <p>
     * By default, type name is equal to simple class name
     * of stored object, but it can depend on implementation
     * of {@link IndexingSpi}.
     *
     * @return Collection of available types.
     */
    public Collection<String> types();

    /**
     * Gets key class name for provided type.
     * <p>
     * Use {@link #types()} method to get available types.
     *
     * @param type Type name.
     * @return Key class name or {@code null} if type name is unknown.
     */
    @Nullable public String keyClass(String type);

    /**
     * Gets value class name for provided type.
     * <p>
     * Use {@link #types()} method to get available types.
     *
     * @param type Type name.
     * @return Value class name or {@code null} if type name is unknown.
     */
    @Nullable public String valueClass(String type);

    /**
     * Gets fields and their class names for provided type.
     *
     * @param type Type name.
     * @return Fields map or {@code null} if type name is unknown.
     */
    @Nullable public Map<String, String> fields(String type);

    /**
     * @return Key classes.
     */
    public Map<String, String> keyClasses();

    /**
     * @return Value classes.
     */
    public Map<String, String> valClasses();

    /**
     * @return Fields.
     */
    public Map<String, Map<String, String>> fields();

    /**
     * @return Indexes.
     */
    public Map<String, Collection<GridCacheSqlIndexMetadata>> indexes();

    /**
     * Gets descriptors of indexes created for provided type.
     * See {@link GridCacheSqlIndexMetadata} javadoc for more information.
     *
     * @param type Type name.
     * @return Index descriptors.
     * @see GridCacheSqlIndexMetadata
     */
    public Collection<GridCacheSqlIndexMetadata> indexes(String type);
}
