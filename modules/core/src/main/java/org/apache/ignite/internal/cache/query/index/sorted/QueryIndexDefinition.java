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

import java.util.LinkedHashMap;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;

/**
 * Define query index.
 */
public class QueryIndexDefinition implements SortedIndexDefinition {
    /** Wrapped key definitions. */
    private final LinkedHashMap<String, IndexKeyDefinition> keyDefs;

    /** Type descriptor. */
    private final GridQueryTypeDescriptor typeDesc;

    /** Cache info. */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** Index name. */
    private final IndexName idxName;

    /** Index tree name. */
    private final String treeName;

    /** Configured inline size. */
    private final int inlineSize;

    /** Segments. */
    private final int segments;

    /** Whether this index is primary key (unique) or not. */
    private final boolean isPrimary;

    /** Whether this index is affinity key index or not. */
    private final boolean isAffinity;

    /** Index row comparator. */
    private final IndexRowComparator rowComparator;

    /** Index key type settings. */
    private final IndexKeyTypeSettings keyTypeSettings;

    /** Index rows cache. */
    private final IndexRowCache idxRowCache;

    /** Row handler factory. */
    private final QueryIndexRowHandlerFactory rowHndFactory = new QueryIndexRowHandlerFactory();

    /** */
    public QueryIndexDefinition(
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        IndexName idxName,
        String treeName,
        IndexRowCache idxRowCache,
        boolean isPrimary,
        boolean isAffinity,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        int cfgInlineSize,
        IndexKeyTypeSettings keyTypeSettings
    ) {
        this.typeDesc = typeDesc;
        this.cacheInfo = cacheInfo;
        this.idxName = idxName;
        this.treeName = treeName;
        this.idxRowCache = idxRowCache;
        this.segments = cacheInfo.cacheContext().config().getQueryParallelism();
        this.inlineSize = cfgInlineSize;
        this.isPrimary = isPrimary;
        this.isAffinity = isAffinity;
        this.keyDefs = keyDefs;
        this.keyTypeSettings = keyTypeSettings;
        this.rowComparator = new IndexRowComparatorImpl(keyTypeSettings);
    }

    /** {@inheritDoc} */
    @Override public String treeName() {
        return treeName;
    }

    /** {@inheritDoc} */
    @Override public LinkedHashMap<String, IndexKeyDefinition> indexKeyDefinitions() {
        return keyDefs;
    }

    /** {@inheritDoc} */
    @Override public IndexRowComparator rowComparator() {
        return rowComparator;
    }

    /** {@inheritDoc} */
    @Override public int segments() {
        return segments;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        return isPrimary;
    }

    /** {@inheritDoc} */
    @Override public boolean affinity() {
        return isAffinity;
    }

    /** {@inheritDoc} */
    @Override public InlineIndexRowHandlerFactory rowHandlerFactory() {
        return rowHndFactory;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyTypeSettings keyTypeSettings() {
        return keyTypeSettings;
    }

    /** {@inheritDoc} */
    @Override public IndexRowCache idxRowCache() {
        return idxRowCache;
    }

    /** {@inheritDoc} */
    @Override public IndexName idxName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptor() {
        return typeDesc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContextInfo<?, ?> cacheInfo() {
        return cacheInfo;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String flds = String.join(", ", indexKeyDefinitions().keySet());

        return "QueryIndex[name=" + idxName.idxName() + ", fields=" + flds + "]";
    }
}
