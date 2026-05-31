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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.database.H2PkHashIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Index;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;

/**
 * Information about table in database.
 */
public class H2TableDescriptor {
    /** PK index name. */
    public static final String PK_IDX_NAME = QueryUtils.PRIMARY_KEY_INDEX;

    /** PK hash index name. */
    public static final String PK_HASH_IDX_NAME = "_key_PK_hash";

    /** Affinity key index name. */
    public static final String AFFINITY_KEY_IDX_NAME = QueryUtils.AFFINITY_KEY_INDEX;

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** */
    private final String fullTblName;

    /** */
    private final GridQueryTypeDescriptor type;

    /** Schema name. */
    private final String schemaName;

    /** Cache context info. */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** */
    private GridH2Table tbl;

    /** */
    private GridLuceneIndex luceneIdx;

    /** */
    private H2PkHashIndex pkHashIdx;

    /**
     * Constructor.
     *
     * @param idx Indexing.
     * @param schemaName Schema name.
     * @param type Type descriptor.
     * @param cacheInfo Cache context info.
     */
    public H2TableDescriptor(
        IgniteH2Indexing idx,
        String schemaName,
        GridQueryTypeDescriptor type,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        this.idx = idx;
        this.type = type;
        this.schemaName = schemaName;
        this.cacheInfo = cacheInfo;

        fullTblName = H2Utils.withQuotes(schemaName) + "." + H2Utils.withQuotes(type.tableName());
    }

    /**
     * @return Indexing.
     */
    public IgniteH2Indexing indexing() {
        return idx;
    }

    /**
     * @return Table.
     */
    public GridH2Table table() {
        return tbl;
    }

    /**
     * @param tbl Table.
     */
    public void table(GridH2Table tbl) {
        this.tbl = tbl;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    String tableName() {
        return type.tableName();
    }

    /**
     * @return Database full table name.
     */
    String fullTableName() {
        return fullTblName;
    }

    /**
     * @return type name.
     */
    String typeName() {
        return type.name();
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheInfo.name();
    }

    /**
     * @return Cache context info.
     */
    public GridCacheContextInfo<?, ?> cacheInfo() {
        return cacheInfo;
    }

    /**
     * @return Type.
     */
    public GridQueryTypeDescriptor type() {
        return type;
    }

    /**
     * @return Lucene index.
     */
    public GridLuceneIndex luceneIndex() {
        return luceneIdx;
    }

    /**
     * @return Hash index.
     */
    public Index hashIndex() {
        return pkHashIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TableDescriptor.class, this);
    }

    /**
     * Create hash index if needed.
     */
    public void createHashIndex(GridH2Table tbl) {
        if (cacheInfo.affinityNode()) {
            IndexColumn keyCol = tbl.indexColumn(QueryUtils.KEY_COL, SortOrder.ASCENDING);
            IndexColumn affCol = tbl.getAffinityKeyColumn();

            if (affCol != null && H2Utils.equals(affCol, keyCol))
                affCol = null;

            List<IndexColumn> cols = affCol == null ? Collections.singletonList(keyCol) : F.asList(keyCol, affCol);

            assert pkHashIdx == null : pkHashIdx;

            pkHashIdx = new H2PkHashIndex(cacheInfo.cacheContext(), tbl, PK_HASH_IDX_NAME, cols,
                tbl.rowDescriptor().context().config().getQueryParallelism());
        }
    }

    /**
     * Create text (lucene) index if needed.
     */
    public void createTextIndex(GridH2Table tbl) {
        if (type().valueClass() == String.class
            && !idx.distributedConfiguration().isDisableCreateLuceneIndexForStringValueType()) {
            try {
                luceneIdx = new GridLuceneIndex(idx.kernalContext(), tbl.cacheName(), type);
            }
            catch (IgniteCheckedException e1) {
                throw new IgniteException(e1);
            }
        }

        GridQueryIndexDescriptor textIdx = type.textIndex();

        if (textIdx != null) {
            try {
                luceneIdx = new GridLuceneIndex(idx.kernalContext(), tbl.cacheName(), type);
            }
            catch (IgniteCheckedException e1) {
                throw new IgniteException(e1);
            }
        }
    }

    /**
     * Handle drop.
     */
    void onDrop() {
        tbl.destroy();

        U.closeQuiet(luceneIdx);
    }
}
