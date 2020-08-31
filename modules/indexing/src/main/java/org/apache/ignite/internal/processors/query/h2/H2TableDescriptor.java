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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.database.H2PkHashIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Index;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.jetbrains.annotations.NotNull;

/**
 * Information about table in database.
 */
public class H2TableDescriptor {
    /** PK index name. */
    public static final String PK_IDX_NAME = "_key_PK";

    /** PK hash index name. */
    public static final String PK_HASH_IDX_NAME = "_key_PK_hash";

    /** Affinity key index name. */
    public static final String AFFINITY_KEY_IDX_NAME = "AFFINITY_KEY";

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** */
    private final String fullTblName;

    /** */
    private final GridQueryTypeDescriptor type;

    /** Schema name. */
    private final String schemaName;

    /** Cache context info. */
    private final GridCacheContextInfo cacheInfo;

    /** */
    private GridH2Table tbl;

    /** */
    private GridLuceneIndex luceneIdx;

    /** */
    private H2PkHashIndex pkHashIdx;

    /** Flag of table has been created from SQL*/
    private boolean isSql;

    /**
     * Constructor.
     *
     * @param idx Indexing.
     * @param schemaName Schema name.
     * @param type Type descriptor.
     * @param cacheInfo Cache context info.
     * @param isSql {@code true} in case table has been created from SQL.
     */
    public H2TableDescriptor(IgniteH2Indexing idx, String schemaName, GridQueryTypeDescriptor type,
        GridCacheContextInfo cacheInfo, boolean isSql) {
        this.idx = idx;
        this.type = type;
        this.schemaName = schemaName;
        this.cacheInfo = cacheInfo;
        this.isSql = isSql;

        fullTblName = H2Utils.withQuotes(schemaName) + "." + H2Utils.withQuotes(type.tableName());
    }

    /**
     * @return {@code true} In case table was created from SQL.
     */
    public boolean sql() {
        return isSql;
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
    public GridCacheContextInfo cacheInfo() {
        return cacheInfo;
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext cache() {
        return cacheInfo.cacheContext();
    }

    /**
     * @return Type.
     */
    GridQueryTypeDescriptor type() {
        return type;
    }

    /**
     * @return Lucene index.
     */
    GridLuceneIndex luceneIndex() {
        return luceneIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TableDescriptor.class, this);
    }

    /**
     * Create list of indexes. First must be primary key, after that all unique indexes and only then non-unique
     * indexes. All indexes must be subtypes of {@link H2TreeIndexBase}.
     *
     * @param tbl Table to create indexes for.
     * @return List of indexes.
     */
    public ArrayList<Index> createSystemIndexes(GridH2Table tbl) {
        ArrayList<Index> idxs = new ArrayList<>();

        IndexColumn keyCol = tbl.indexColumn(QueryUtils.KEY_COL, SortOrder.ASCENDING);
        IndexColumn affCol = tbl.getAffinityKeyColumn();

        if (affCol != null && H2Utils.equals(affCol, keyCol))
            affCol = null;

        List<IndexColumn> unwrappedKeyAndAffinityCols = extractKeyColumns(tbl, keyCol, affCol);

        List<IndexColumn> wrappedKeyCols = H2Utils.treeIndexColumns(tbl.rowDescriptor(),
            new ArrayList<>(2), keyCol, affCol);

        Index hashIdx = createHashIndex(
            tbl,
            wrappedKeyCols
        );

        if (hashIdx != null)
            idxs.add(hashIdx);

        // Add primary key index.
        Index pkIdx = idx.createSortedIndex(
            PK_IDX_NAME,
            tbl,
            true,
            false,
            unwrappedKeyAndAffinityCols,
            wrappedKeyCols,
            -1
        );

        idxs.add(pkIdx);

        if (type().valueClass() == String.class) {
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

        // Locate index where affinity column is first (if any).
        if (affCol != null) {
            boolean affIdxFound = false;

            for (GridQueryIndexDescriptor idxDesc : type.indexes().values()) {
                if (idxDesc.type() != QueryIndexType.SORTED)
                    continue;

                String firstField = idxDesc.fields().iterator().next();

                Column col = tbl.getColumn(firstField);

                IndexColumn idxCol = tbl.indexColumn(col.getColumnId(),
                    idxDesc.descending(firstField) ? SortOrder.DESCENDING : SortOrder.ASCENDING);

                affIdxFound |= H2Utils.equals(idxCol, affCol);
            }

            // Add explicit affinity key index if nothing alike was found.
            if (!affIdxFound) {
                List<IndexColumn> unwrappedKeyCols = extractKeyColumns(tbl, keyCol, null);

                ArrayList<IndexColumn> colsWithUnwrappedKey = new ArrayList<>(unwrappedKeyCols.size());

                colsWithUnwrappedKey.add(affCol);

                //We need to reorder PK columns to have affinity key as first column, that's why we can't use simple PK columns
                H2Utils.addUniqueColumns(colsWithUnwrappedKey, unwrappedKeyCols);

                List<IndexColumn> cols = H2Utils.treeIndexColumns(tbl.rowDescriptor(), new ArrayList<>(2), affCol, keyCol);

                idxs.add(idx.createSortedIndex(
                    AFFINITY_KEY_IDX_NAME,
                    tbl,
                    false,
                    true,
                    colsWithUnwrappedKey,
                    cols,
                    -1)
                );
            }
        }

        return idxs;
    }

    /**
     * Create list of affinity and key index columns. Key, if it possible, partitions into simple components.
     *
     * @param tbl GridH2Table instance
     * @param keyCol Key index column.
     * @param affCol Affinity index column.
     *
     * @return List of key and affinity columns. Key's, if it possible, splitted into simple components.
     */
    @NotNull private List<IndexColumn> extractKeyColumns(GridH2Table tbl, IndexColumn keyCol, IndexColumn affCol) {
        ArrayList<IndexColumn> keyCols;

        if (isSql) {
            keyCols = new ArrayList<>(type.fields().size() + 1);

            // Check if key is simple type.
            if (QueryUtils.isSqlType(type.keyClass()))
                keyCols.add(keyCol);
            else {
                // SPECIFIED_SEQ_PK_KEYS check guarantee that request running on heterogeneous (RU) cluster can
                // perform equally on all nodes.
                if (!idx.kernalContext().recoveryMode()) {
                    for (String keyName : type.primaryKeyFields()) {
                        GridQueryProperty prop = type.property(keyName);

                        assert prop.key() : keyName + " is not a key field";

                        Column col = tbl.getColumn(prop.name());

                        keyCols.add(tbl.indexColumn(col.getColumnId(), SortOrder.ASCENDING));
                    }
                }
                else {
                    for (String propName : type.fields().keySet()) {
                        GridQueryProperty prop = type.property(propName);

                        if (prop.key()) {
                            Column col = tbl.getColumn(propName);

                            keyCols.add(tbl.indexColumn(col.getColumnId(), SortOrder.ASCENDING));
                        }
                    }
                }

                // If key is object but the user has not specified any particular columns,
                // we have to fall back to whole-key index.
                if (keyCols.isEmpty())
                    keyCols.add(keyCol);
            }

        }
        else {
            keyCols = new ArrayList<>(2);

            keyCols.add(keyCol);
        }

        if (affCol != null && !H2Utils.containsColumn(keyCols, affCol))
            keyCols.add(affCol);
        else
            keyCols.trimToSize();

        return Collections.unmodifiableList(keyCols);
    }

    /**
     * Get collection of user indexes.
     *
     * @return User indexes.
     */
    public Collection<GridH2IndexBase> createUserIndexes() {
        assert tbl != null;

        ArrayList<GridH2IndexBase> res = new ArrayList<>();

        for (GridQueryIndexDescriptor idxDesc : type.indexes().values()) {
            GridH2IndexBase idx = createUserIndex(idxDesc);

            res.add(idx);
        }

        return res;
    }

    /**
     * Create user index.
     *
     * @param idxDesc Index descriptor.
     * @return Index.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public GridH2IndexBase createUserIndex(GridQueryIndexDescriptor idxDesc) {
        IndexColumn keyCol = tbl.indexColumn(QueryUtils.KEY_COL, SortOrder.ASCENDING);
        IndexColumn affCol = tbl.getAffinityKeyColumn();

        List<IndexColumn> cols = new ArrayList<>(idxDesc.fields().size() + 2);

        for (String field : idxDesc.fields()) {
            Column col = tbl.getColumn(field);

            cols.add(tbl.indexColumn(col.getColumnId(),
                idxDesc.descending(field) ? SortOrder.DESCENDING : SortOrder.ASCENDING));
        }

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (idxDesc.type() == QueryIndexType.SORTED) {
            List<IndexColumn> unwrappedKeyCols = extractKeyColumns(tbl, keyCol, affCol);

            List<IndexColumn> colsWithUnwrappedKey = new ArrayList<>(cols);

            H2Utils.addUniqueColumns(colsWithUnwrappedKey, unwrappedKeyCols);

            cols = H2Utils.treeIndexColumns(desc, cols, keyCol, affCol);

            return idx.createSortedIndex(
                idxDesc.name(),
                tbl,
                false,
                false,
                colsWithUnwrappedKey,
                cols,
                idxDesc.inlineSize()
            );
        }
        else if (idxDesc.type() == QueryIndexType.GEOSPATIAL)
            return H2Utils.createSpatialIndex(tbl, idxDesc.name(), cols.toArray(new IndexColumn[0]));

        throw new IllegalStateException("Index type: " + idxDesc.type());
    }

    /**
     * Create hash index.
     *
     * @param tbl Table.
     * @param cols Columns.
     * @return Index.
     */
    private Index createHashIndex(GridH2Table tbl, List<IndexColumn> cols) {
        if (cacheInfo.affinityNode()) {
            assert pkHashIdx == null : pkHashIdx;

            pkHashIdx = new H2PkHashIndex(cacheInfo.cacheContext(), tbl, PK_HASH_IDX_NAME, cols,
                tbl.rowDescriptor().context().config().getQueryParallelism());

            return pkHashIdx;
        }

        return null;
    }

    /**
     * Handle drop.
     */
    void onDrop() {
        tbl.destroy();

        U.closeQuiet(luceneIdx);
    }
}
