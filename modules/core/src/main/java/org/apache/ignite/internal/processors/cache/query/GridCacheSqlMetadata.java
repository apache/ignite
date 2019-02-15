/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     * Gets not null fields.
     *
     * @param type Type name.
     * @return Not null fields collection map or {@code null} if type name is unknown.
     */
    Collection<String> notNullFields(String type);

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
