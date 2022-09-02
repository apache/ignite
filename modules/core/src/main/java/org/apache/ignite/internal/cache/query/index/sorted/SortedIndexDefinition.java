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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;

/**
 * Represents a definition of a sorted index.
 */
public interface SortedIndexDefinition extends IndexDefinition {
    /** Represents an index tree name. */
    public String treeName();

    /** Comparator for comparing index rows. */
    public IndexRowComparator rowComparator();

    /** Index row handler. */
    public InlineIndexRowHandlerFactory rowHandlerFactory();

    /** Index key type settings. */
    public IndexKeyTypeSettings keyTypeSettings();

    /** Cache of index rows. */
    public IndexRowCache idxRowCache();

    /** Type descriptor. */
    public GridQueryTypeDescriptor typeDescriptor();

    /** Cache info. */
    public GridCacheContextInfo<?, ?> cacheInfo();

    /** Amount of index tree segments.*/
    public int segments();

    /** Inline size. */
    public int inlineSize();

    /** Whether this index is primary key (unique) or not. */
    public boolean primary();

    /** Whether this index is affinity key index or not. */
    public boolean affinity();
}
