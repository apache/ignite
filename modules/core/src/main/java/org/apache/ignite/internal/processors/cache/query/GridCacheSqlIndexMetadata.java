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

/**
 * Ignite index descriptor.
 * <p>
 * All index descriptors can be obtained from
 * {@link GridCacheSqlMetadata#indexes(String)} method.
 * @see GridCacheSqlMetadata
 */
public interface GridCacheSqlIndexMetadata extends Externalizable {
    /**
     * Gets name of the index.
     *
     * @return Index name.
     */
    public String name();

    /**
     * Gets names of fields indexed by this index.
     *
     * @return Indexed fields names.
     */
    public Collection<String> fields();

    /**
     * Gets order of the index for each indexed field.
     *
     * @param field Field name.
     * @return {@code True} if given field is indexed in descending order.
     */
    public boolean descending(String field);

    /**
     * @return Descendings.
     */
    public Collection<String> descendings();

    /**
     * Gets whether this is a unique index.
     *
     * @return {@code True} if index is unique, {@code false} otherwise.
     */
    public boolean unique();
}
