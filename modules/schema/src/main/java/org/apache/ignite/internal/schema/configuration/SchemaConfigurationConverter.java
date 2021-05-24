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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnChange;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesChange;
import org.apache.ignite.configuration.tree.NamedListView;
import org.apache.ignite.internal.schema.ColumnImpl;
import org.apache.ignite.internal.schema.HashIndexImpl;
import org.apache.ignite.internal.schema.PartialIndexImpl;
import org.apache.ignite.internal.schema.PrimaryIndexImpl;
import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.internal.schema.SortedIndexColumnImpl;
import org.apache.ignite.internal.schema.SortedIndexImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.SortedIndexColumn;
import org.apache.ignite.schema.TableIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Configuration to schema and vice versa converter.
 */
public class SchemaConfigurationConverter {
    /** Hash index type. */
    private static final String HASH_TYPE = "HASH";

    /** Sorted index type. */
    private static final String SORTED_TYPE = "SORTED";

    /** Partial index type. */
    private static final String PARTIAL_TYPE = "PARTIAL";

    /** Primary key index type. */
    private static final String PK_TYPE = "PK";

    /** Types map. */
    private static final Map<String, ColumnType> types = new HashMap<>();

    static {
        putType(ColumnType.INT8);
        putType(ColumnType.INT16);
        putType(ColumnType.INT32);
        putType(ColumnType.INT64);
        putType(ColumnType.UINT8);
        putType(ColumnType.UINT16);
        putType(ColumnType.UINT32);
        putType(ColumnType.UINT64);
        putType(ColumnType.FLOAT);
        putType(ColumnType.DOUBLE);
        putType(ColumnType.UUID);
    }

    /** */
    private static void putType(ColumnType type) {
        types.put(type.typeSpec().name(), type);
    }

    /**
     * Convert SortedIndexColumn to IndexColumnChange.
     * @param col IndexColumnChange.
     * @param colInit IndexColumnChange to fullfill.
     */
    public static void convert(SortedIndexColumn col, IndexColumnChange colInit) {
        colInit.changeName(col.name());
        colInit.changeAsc(col.asc());
    }

    /**
     * Convert IndexColumnView to SortedIndexColumn.
     *
     * @param colCfg IndexColumnView.
     * @return SortedIndexColumn.
     */
    public static SortedIndexColumn convert(IndexColumnView colCfg) {
        return new SortedIndexColumnImpl(colCfg.name(), colCfg.asc());
    }

    /**
     * Convert TableIndex to TableIndexChange.
     *
     * @param idx TableIndex.
     * @param idxChg TableIndexChange to fullfill.
     */
    public static void convert(TableIndex idx, TableIndexChange idxChg) {
        idxChg.changeName(idx.name());
        idxChg.changeType(idx.type());

        switch (idx.type().toUpperCase()) {
            case HASH_TYPE:
                HashIndex hashIdx = (HashIndex)idx;

                String[] colNames = hashIdx.columns().stream().map(IndexColumn::name).toArray(String[]::new);

                idxChg.changeColNames(colNames);

                break;

            case PARTIAL_TYPE:
                PartialIndex partIdx = (PartialIndex)idx;

                idxChg.changeUniq(partIdx.unique());
                idxChg.changeExpr(partIdx.expr());

                idxChg.changeColumns(colsChg -> {
                    int colIdx = 0;

                    for (SortedIndexColumn col : partIdx.columns())
                        colsChg.create(String.valueOf(colIdx++), colInit -> convert(col, colInit));
                });

                break;

            case SORTED_TYPE:
                SortedIndex sortIdx = (SortedIndex)idx;
                idxChg.changeUniq(sortIdx.unique());

                idxChg.changeColumns(colsInit -> {
                    int colIdx = 0;

                    for (SortedIndexColumn col : sortIdx.columns())
                        colsInit.create(String.valueOf(colIdx++), colInit -> convert(col, colInit));
                });

                break;

            case PK_TYPE:
                PrimaryIndex primIdx = (PrimaryIndex)idx;

                idxChg.changeColumns(colsInit -> {
                    int colIdx = 0;

                    for (SortedIndexColumn col : primIdx.columns())
                        colsInit.create(String.valueOf(colIdx++), colInit -> convert(col, colInit));
                });

                idxChg.changeAffinityColumns(primIdx.affinityColumns().toArray(
                    new String[primIdx.affinityColumns().size()]));

                break;

            default:
                throw new IllegalArgumentException("Unknown index type " + idx.type());
        }
    }

    /**
     * Convert TableIndexView into TableIndex.
     *
     * @param idxView TableIndexView.
     * @return TableIndex.
     */
    public static TableIndex convert(TableIndexView idxView) {
        String name = idxView.name();
        String type = idxView.type();

        switch (type.toUpperCase()) {
            case HASH_TYPE:
                String[] hashCols = idxView.colNames();

                return new HashIndexImpl(name, hashCols);

            case SORTED_TYPE:
                boolean sortedUniq = idxView.uniq();

                SortedMap<Integer, SortedIndexColumn> sortedCols = new TreeMap<>();
                
                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumn col = convert(idxView.columns().get(key));
                    
                    sortedCols.put(Integer.valueOf(key), col);
                }

                return new SortedIndexImpl(name, new ArrayList<>(sortedCols.values()), sortedUniq);

            case PARTIAL_TYPE:
                boolean partialUniq = idxView.uniq();
                String expr = idxView.expr();

                NamedListView<? extends IndexColumnView> colsView = idxView.columns();
                SortedMap<Integer, SortedIndexColumn> partialCols = new TreeMap<>();
                
                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumn col = convert(colsView.get(key));
                    
                    partialCols.put(Integer.valueOf(key), col);
                }

                return new PartialIndexImpl(name, new ArrayList<>(partialCols.values()), partialUniq, expr);

            case PK_TYPE:
                SortedMap<Integer, SortedIndexColumn> cols = new TreeMap<>();
                
                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumn col = convert(idxView.columns().get(key));
                    
                    cols.put(Integer.valueOf(key), col);
                }

                String[] affCols = idxView.affinityColumns();

                return new PrimaryIndexImpl(new ArrayList<>(cols.values()), List.of(affCols));

            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    /**
     * Convert ColumnType to ColumnTypeChange.
     *
     * @param colType ColumnType.
     * @param colTypeChg ColumnTypeChange to fullfill.
     */
    public static void convert(ColumnType colType, ColumnTypeChange colTypeChg) {
        String typeName = colType.typeSpec().name().toUpperCase();
        
        if (types.containsKey(typeName))
            colTypeChg.changeType(typeName);
        else {
            colTypeChg.changeType(typeName);

            switch (typeName) {
                case "BITMASK":
                case "BLOB":
                case "STRING":
                    ColumnType.VarLenColumnType varLenColType = (ColumnType.VarLenColumnType)colType;

                    colTypeChg.changeLength(varLenColType.length());

                    break;

                case "DECIMAL":
                    ColumnType.NumericColumnType numColType = (ColumnType.NumericColumnType)colType;

                    colTypeChg.changePrecision(numColType.precision());
                    colTypeChg.changeScale(numColType.scale());

                    break;

                default:
                    throw new IllegalArgumentException("Unknown type " + colType.typeSpec().name());
            }
        }
    }

    /**
     * Convert ColumnTypeView to ColumnType.
     *
     * @param colTypeView ColumnTypeView.
     * @return ColumnType.
     */
    public static ColumnType convert(ColumnTypeView colTypeView) {
        String typeName = colTypeView.type().toUpperCase();
        ColumnType res = types.get(typeName);
        
        if (res != null)
            return res;
        else {
            switch (typeName) {
                case "BITMASK":
                    int bitmaskLen = colTypeView.length();

                    return ColumnType.bitmaskOf(bitmaskLen);

                case "STRING":
                    int strLen = colTypeView.length();

                    return ColumnType.stringOf(strLen);

                case "BLOB":
                    int blobLen = colTypeView.length();

                    return ColumnType.blobOf(blobLen);

                case "DECIMAL":
                    int prec = colTypeView.precision();
                    int scale = colTypeView.scale();

                    return ColumnType.number(prec, scale);

                default:
                    throw new IllegalArgumentException("Unknown type " + typeName);
            }
        }
    }

    /**
     * Convert column to column change.
     *
     * @param col Column to convert.
     * @param colChg Column
     */
    public static void convert(Column col, ColumnChange colChg) {
        colChg.changeName(col.name());
        colChg.changeType(colTypeInit -> convert(col.type(), colTypeInit));
        
        if (col.defaultValue() != null)
            colChg.changeDefaultValue(col.defaultValue().toString());
            
        colChg.changeNullable(col.nullable());
    }

    /**
     * Convert column view to Column.
     *
     * @param colView Column view.
     * @return Column.
     */
    public static Column convert(ColumnView colView) {
        return new ColumnImpl(
            colView.name(),
            convert(colView.type()),
            colView.nullable(),
            colView.defaultValue());
    }

    /**
     * Convert schema table to schema table change.
     *
     * @param tbl Schema table to convert.
     * @param tblChg Change to fullfill.
     */
    public static void convert(SchemaTable tbl, TableChange tblChg) {
        tblChg.changeName(tbl.canonicalName());

        tblChg.changeIndices(idxsChg -> {
            int idxIdx = 0;

            for (TableIndex idx : tbl.indices())
                idxsChg.create(String.valueOf(idxIdx++), idxInit -> convert(idx, idxInit));
        });

        tblChg.changeColumns(colsChg -> {
            int colIdx = 0;

            for (Column col : tbl.keyColumns())
                colsChg.create(String.valueOf(colIdx++), colChg -> convert(col, colChg));

            for (Column col : tbl.valueColumns())
                colsChg.create(String.valueOf(colIdx++), colChg -> convert(col, colChg));
        });
    }

    /**
     * Convert TableConfiguration to SchemaTable.
     *
     * @param tblCfg TableConfiguration to convert.
     * @return SchemaTable.
     */
    public static SchemaTable convert(TableConfiguration tblCfg) {
        return convert(tblCfg.value());
    }

    /**
     * Convert configuration to SchemaTable.
     *
     * @param tblView TableView to convert.
     * @return SchemaTable.
     */
    public static SchemaTableImpl convert(TableView tblView) {
        String canonicalName = tblView.name();
        int sepPos = canonicalName.indexOf('.');
        String schemaName = canonicalName.substring(0, sepPos);
        String tableName = canonicalName.substring(sepPos + 1);

        NamedListView<? extends ColumnView> colsView = tblView.columns();

        SortedMap<Integer, Column> columns = new TreeMap<>();

        for (String key : colsView.namedListKeys()) {
            ColumnView colView = colsView.get(key);
            Column col = convert(colView);

            columns.put(Integer.valueOf(key), col);
        }

        NamedListView<? extends TableIndexView> idxsView = tblView.indices();

        Map<String, TableIndex> indices = new HashMap<>(idxsView.size());

        for (String key : idxsView.namedListKeys()) {
            TableIndexView idxView = idxsView.get(key);
            TableIndex idx = convert(idxView);

            indices.put(idx.name(), idx);
        }

        LinkedHashMap<String, Column> colsMap = new LinkedHashMap<>(colsView.size());
        
        columns.forEach((i,v) -> colsMap.put(v.name(), v));

        return new SchemaTableImpl(schemaName, tableName, colsMap, indices);
    }

    /**
     * Create table.
     *
     * @param tbl Table to create.
     * @param tblsChange Tables change to fulfill.
     * @return TablesChange to get result from.
     */
    public static TablesChange createTable(SchemaTable tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(tblsChg -> tblsChg.create(tbl.canonicalName(), tblChg -> convert(tbl, tblChg)));
    }

    /**
     * Drop table.
     *
     * @param tbl table to drop.
     * @param tblsChange TablesChange change to fulfill.
     * @return TablesChange to get result from.
     */
    public static TablesChange dropTable(SchemaTable tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(schmTblChange -> schmTblChange.delete(tbl.canonicalName()));
    }

    /**
     * Add index.
     *
     * @param idx Index to add.
     * @param tblChange TableChange to fullfill.
     * @return TableChange to get result from.
     */
    public static TableChange addIndex(TableIndex idx, TableChange tblChange) {
        return tblChange.changeIndices(idxsChg -> idxsChg.create(idx.name(), idxChg -> convert(idx, idxChg)));
    }

    /**
     * Drop index.
     *
     * @param indexName Index name to drop.
     * @param tblChange Table change to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange dropIndex(String indexName, TableChange tblChange) {
        return tblChange.changeIndices(idxChg -> idxChg.delete(indexName));
    }

    /**
     * Add table column.
     *
     * @param column Column to add.
     * @param tblChange TableChange to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange addColumn(Column column, TableChange tblChange) {
        return tblChange.changeColumns(colsChg -> colsChg.create(column.name(), colChg -> convert(column, colChg)));
    }

    /**
     * Drop table column.
     *
     * @param columnName column name to drop.
     * @param tblChange TableChange to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange dropColumn(String columnName, TableChange tblChange) {
        return tblChange.changeColumns(colChg -> colChg.delete(columnName));
    }
}
