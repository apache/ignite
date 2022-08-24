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

package org.apache.ignite.internal.processors.query.schema.management;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Local database table object.
 */
public class TableDescriptor {
    /** */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** */
    private final GridQueryTypeDescriptor typeDesc;

    /** */
    private final boolean isSql;

    /** */
    private final Map<String, IndexDescriptor> idxs = new ConcurrentHashMap<>();

    /** */
    private final String affKey;

    /** */
    private volatile boolean idxRebuildInProgress;

    /**
     * Ctor.
     *
     * @param cacheInfo Cache cacheInfo context.
     * @param typeDesc Descriptor.
     */
    public TableDescriptor(GridCacheContextInfo<?, ?> cacheInfo, GridQueryTypeDescriptor typeDesc, boolean isSql) {
        this.cacheInfo = cacheInfo;
        this.typeDesc = typeDesc;
        this.isSql = isSql;

        if (!F.isEmpty(typeDesc.affinityKey())
            && !typeDesc.customAffinityKeyMapper()
            && !F.eq(typeDesc.affinityKey(), QueryUtils.KEY_FIELD_NAME)
            && !F.eq(typeDesc.affinityKey(), typeDesc.keyFieldName())
            && typeDesc.fields().containsKey(typeDesc.affinityKey())
        )
            affKey = typeDesc.affinityKey();
        else
            affKey = null;
    }

    /** */
    public GridCacheContextInfo<?, ?> cacheInfo() {
        return cacheInfo;
    }

    /** */
    public GridQueryTypeDescriptor type() {
        return typeDesc;
    }

    /** */
    public boolean isSql() {
        return isSql;
    }

    /** */
    void addIndex(String idxName, IndexDescriptor idx) {
        idxs.put(idxName, idx);
    }

    /** */
    IndexDescriptor dropIndex(String idxName) {
        return idxs.remove(idxName);
    }

    /** */
    Map<String, IndexDescriptor> indexes() {
        return Collections.unmodifiableMap(idxs);
    }

    /** */
    public String affinityKey() {
        return affKey;
    }

    /** */
    public boolean isIndexRebuildInProgress() {
        return idxRebuildInProgress;
    }

    /** */
    void markIndexRebuildInProgress(boolean idxRebuildInProgress) {
        this.idxRebuildInProgress = idxRebuildInProgress;
    }
}
