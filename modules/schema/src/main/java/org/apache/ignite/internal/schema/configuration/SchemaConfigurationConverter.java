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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeChange;
import org.apache.ignite.configuration.schemas.table.ColumnTypeView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnChange;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.PrimaryKeyView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesChange;
import org.apache.ignite.internal.schema.definition.ColumnDefinitionImpl;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.HashIndexDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.PartialIndexDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.PrimaryKeyDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.SortedIndexColumnDefinitionImpl;
import org.apache.ignite.internal.schema.definition.index.SortedIndexDefinitionImpl;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;
import org.apache.ignite.schema.definition.index.PartialIndexDefinition;
import org.apache.ignite.schema.definition.index.SortOrder;
import org.apache.ignite.schema.definition.index.SortedIndexColumnDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;

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
        putType(ColumnType.DATE);
    }

    /**
     * @param type Column type.
     */
    private static void putType(ColumnType type) {
        types.put(type.typeSpec().name(), type);
    }

    /**
     * Convert SortedIndexColumn to IndexColumnChange.
     *
     * @param col IndexColumnChange.
     * @param colInit IndexColumnChange to fulfill.
     * @return IndexColumnChange to get result from.
     */
    public static IndexColumnChange convert(SortedIndexColumnDefinition col, IndexColumnChange colInit) {
        colInit.changeName(col.name());

        colInit.changeAsc(col.sortOrder() == SortOrder.ASC);

        return colInit;
    }

    /**
     * Convert IndexColumnView to SortedIndexColumn.
     *
     * @param colCfg IndexColumnView.
     * @return SortedIndexColumn.
     */
    public static SortedIndexColumnDefinition convert(IndexColumnView colCfg) {
        return new SortedIndexColumnDefinitionImpl(colCfg.name(), colCfg.asc() ? SortOrder.ASC : SortOrder.DESC);
    }

    /**
     * Convert TableIndex to TableIndexChange.
     *
     * @param idx TableIndex.
     * @param idxChg TableIndexChange to fulfill.
     * @return TableIndexChange to get result from.
     */
    public static TableIndexChange convert(IndexDefinition idx, TableIndexChange idxChg) {
        idxChg.changeName(idx.name());
        idxChg.changeType(idx.type());

        switch (idx.type().toUpperCase()) {
            case HASH_TYPE:
                HashIndexDefinition hashIdx = (HashIndexDefinition)idx;

                String[] colNames = hashIdx.columns().stream().map(IndexColumnDefinition::name).toArray(String[]::new);

                idxChg.changeColNames(colNames);

                break;

            case PARTIAL_TYPE:
                PartialIndexDefinition partIdx = (PartialIndexDefinition)idx;

                idxChg.changeUniq(partIdx.unique());
                idxChg.changeExpr(partIdx.expr());

                idxChg.changeColumns(colsChg -> {
                    int colIdx = 0;

                    for (SortedIndexColumnDefinition col : partIdx.columns())
                        colsChg.create(String.valueOf(colIdx++), colInit -> convert(col, colInit));
                });

                break;

            case SORTED_TYPE:
                SortedIndexDefinition sortIdx = (SortedIndexDefinition)idx;
                idxChg.changeUniq(sortIdx.unique());

                idxChg.changeColumns(colsInit -> {
                    int colIdx = 0;

                    for (SortedIndexColumnDefinition col : sortIdx.columns())
                        colsInit.create(String.valueOf(colIdx++), colInit -> convert(col, colInit));
                });

                break;

            default:
                throw new IllegalArgumentException("Unknown index type " + idx.type());
        }

        return idxChg;
    }

    /**
     * Convert TableIndexView into TableIndex.
     *
     * @param idxView TableIndexView.
     * @return TableIndex.
     */
    public static IndexDefinition convert(TableIndexView idxView) {
        String name = idxView.name();
        String type = idxView.type();

        switch (type.toUpperCase()) {
            case HASH_TYPE:
                String[] hashCols = idxView.colNames();

                return new HashIndexDefinitionImpl(name, hashCols);

            case SORTED_TYPE:
                SortedMap<Integer, SortedIndexColumnDefinition> sortedCols = new TreeMap<>();

                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumnDefinition col = convert(idxView.columns().get(key));

                    sortedCols.put(Integer.valueOf(key), col);
                }

                return new SortedIndexDefinitionImpl(name, new ArrayList<>(sortedCols.values()), idxView.uniq());

            case PARTIAL_TYPE:
                String expr = idxView.expr();

                NamedListView<? extends IndexColumnView> colsView = idxView.columns();
                SortedMap<Integer, SortedIndexColumnDefinition> partialCols = new TreeMap<>();

                for (String key : idxView.columns().namedListKeys()) {
                    SortedIndexColumnDefinition col = convert(colsView.get(key));

                    partialCols.put(Integer.valueOf(key), col);
                }

                return new PartialIndexDefinitionImpl(name, new ArrayList<>(partialCols.values()), expr, idxView.uniq());

            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    /**
     * Convert PrimaryKeyView into PrimaryKey.
     *
     * @param primaryKey PrimaryKeyView.
     * @return TableIn.
     */
    public static PrimaryKeyDefinition convert(PrimaryKeyView primaryKey) {
        return new PrimaryKeyDefinitionImpl(Set.of(primaryKey.columns()), Set.of(primaryKey.affinityColumns()));
    }

    /**
     * Convert ColumnType to ColumnTypeChange.
     *
     * @param colType ColumnType.
     * @param colTypeChg ColumnTypeChange to fulfill.
     * @return ColumnTypeChange to get result from
     */
    public static ColumnTypeChange convert(ColumnType colType, ColumnTypeChange colTypeChg) {
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
                    ColumnType.DecimalColumnType numColType = (ColumnType.DecimalColumnType)colType;

                    colTypeChg.changePrecision(numColType.precision());
                    colTypeChg.changeScale(numColType.scale());

                    break;

                case "NUMBER":
                    ColumnType.NumberColumnType numType = (ColumnType.NumberColumnType)colType;

                    colTypeChg.changePrecision(numType.precision());

                    break;

                case "TIME":
                case "DATETIME":
                case "TIMESTAMP":
                    ColumnType.TemporalColumnType temporalColType = (ColumnType.TemporalColumnType)colType;

                    colTypeChg.changePrecision(temporalColType.precision());

                    break;

                default:
                    throw new IllegalArgumentException("Unknown type " + colType.typeSpec().name());
            }
        }

        return colTypeChg;
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

                    return ColumnType.decimalOf(prec, scale);

                case "NUMBER":
                    return ColumnType.numberOf(colTypeView.precision());

                case "TIME":
                    return ColumnType.time(colTypeView.precision());

                case "DATETIME":
                    return ColumnType.datetime(colTypeView.precision());

                case "TIMESTAMP":
                    return ColumnType.timestamp(colTypeView.precision());

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
     * @return ColumnChange to get result from.
     */
    public static ColumnChange convert(ColumnDefinition col, ColumnChange colChg) {
        colChg.changeName(col.name());
        colChg.changeType(colTypeInit -> convert(col.type(), colTypeInit));

        if (col.defaultValue() != null)
            colChg.changeDefaultValue(col.defaultValue().toString());

        colChg.changeNullable(col.nullable());

        return colChg;
    }

    /**
     * Convert column view to Column.
     *
     * @param colView Column view.
     * @return Column.
     */
    public static ColumnDefinition convert(ColumnView colView) {
        return new ColumnDefinitionImpl(
            colView.name(),
            convert(colView.type()),
            colView.nullable(),
            colView.defaultValue());
    }

    /**
     * Convert table schema to table changer.
     *
     * @param tbl Table schema to convert.
     * @param tblChg Change to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange convert(TableDefinition tbl, TableChange tblChg) {
        tblChg.changeName(tbl.canonicalName());

        tblChg.changeIndices(idxsChg -> {
            int idxIdx = 0;

            for (IndexDefinition idx : tbl.indices())
                idxsChg.create(String.valueOf(idxIdx++), idxInit -> convert(idx, idxInit));
        });

        tblChg.changeColumns(colsChg -> {
            int colIdx = 0;

            for (ColumnDefinition col : tbl.columns())
                colsChg.create(String.valueOf(colIdx++), colChg -> convert(col, colChg));
        });

        tblChg.changePrimaryKey(pkCng -> {
            pkCng.changeColumns(tbl.keyColumns().toArray(String[]::new))
                .changeAffinityColumns(tbl.affinityColumns().toArray(String[]::new));
        });

        return tblChg;
    }

    /**
     * Convert TableConfiguration to TableSchema.
     *
     * @param tblCfg TableConfiguration to convert.
     * @return Table schema.
     */
    public static TableDefinition convert(TableConfiguration tblCfg) {
        return convert(tblCfg.value());
    }

    /**
     * Convert table configuration view to table schema.
     *
     * @param tblView TableView to convert.
     * @return Table schema.
     */
    public static TableDefinitionImpl convert(TableView tblView) {
        String canonicalName = tblView.name();
        int sepPos = canonicalName.indexOf('.');
        String schemaName = canonicalName.substring(0, sepPos);
        String tableName = canonicalName.substring(sepPos + 1);

        NamedListView<? extends ColumnView> colsView = tblView.columns();

        SortedMap<Integer, ColumnDefinition> columns = new TreeMap<>();

        for (String key : colsView.namedListKeys()) {
            ColumnView colView = colsView.get(key);

            if (colView != null) {
                ColumnDefinition col = convert(colView);

                columns.put(Integer.valueOf(key), col);
            }
        }

        NamedListView<? extends TableIndexView> idxsView = tblView.indices();

        Map<String, IndexDefinition> indices = new HashMap<>(idxsView.size());

        for (String key : idxsView.namedListKeys()) {
            TableIndexView idxView = idxsView.get(key);
            IndexDefinition idx = convert(idxView);

            indices.put(idx.name(), idx);
        }

        LinkedHashMap<String, ColumnDefinition> colsMap = new LinkedHashMap<>(colsView.size());

        columns.forEach((i, v) -> colsMap.put(v.name(), v));

        PrimaryKeyDefinition primaryKey = convert(tblView.primaryKey());

        return new TableDefinitionImpl(schemaName, tableName, colsMap, primaryKey, indices);
    }

    /**
     * Create table.
     *
     * @param tbl Table to create.
     * @param tblsChange Tables change to fulfill.
     * @return TablesChange to get result from.
     */
    public static TablesChange createTable(TableDefinition tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(tblsChg -> tblsChg.create(tbl.canonicalName(), tblChg -> convert(tbl, tblChg)));
    }

    /**
     * Drop table.
     *
     * @param tbl table to drop.
     * @param tblsChange TablesChange change to fulfill.
     * @return TablesChange to get result from.
     */
    public static TablesChange dropTable(TableDefinition tbl, TablesChange tblsChange) {
        return tblsChange.changeTables(schmTblChange -> schmTblChange.delete(tbl.canonicalName()));
    }

    /**
     * Add index.
     *
     * @param idx Index to add.
     * @param tblChange TableChange to fulfill.
     * @return TableChange to get result from.
     */
    public static TableChange addIndex(IndexDefinition idx, TableChange tblChange) {
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
    public static TableChange addColumn(ColumnDefinition column, TableChange tblChange) {
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

    /**
     * Gets ColumnType type for given class.
     *
     * @param cls Class.
     * @return ColumnType type or null.
     */
    public static ColumnType columnType(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == byte.class)
            return ColumnType.INT8;
        else if (cls == short.class)
            return ColumnType.INT16;
        else if (cls == int.class)
            return ColumnType.INT32;
        else if (cls == long.class)
            return ColumnType.INT64;
        else if (cls == float.class)
            return ColumnType.FLOAT;
        else if (cls == double.class)
            return ColumnType.DOUBLE;

            // Boxed primitives.
        else if (cls == Byte.class)
            return ColumnType.INT8;
        else if (cls == Short.class)
            return ColumnType.INT16;
        else if (cls == Integer.class)
            return ColumnType.INT32;
        else if (cls == Long.class)
            return ColumnType.INT64;
        else if (cls == Float.class)
            return ColumnType.FLOAT;
        else if (cls == Double.class)
            return ColumnType.DOUBLE;

            // Temporal types.
        else if (cls == LocalDate.class)
            return ColumnType.DATE;
        else if (cls == LocalTime.class)
            return ColumnType.time(ColumnType.TemporalColumnType.DEFAULT_PRECISION);
        else if (cls == LocalDateTime.class)
            return ColumnType.datetime(ColumnType.TemporalColumnType.DEFAULT_PRECISION);
        else if (cls == Instant.class)
            return ColumnType.timestamp(ColumnType.TemporalColumnType.DEFAULT_PRECISION);

            // Other types
        else if (cls == String.class)
            return ColumnType.string();
        else if (cls == UUID.class)
            return ColumnType.UUID;
        else if (cls == BigInteger.class)
            return ColumnType.numberOf();
        else if (cls == BigDecimal.class)
            return ColumnType.decimalOf();

        return null;
    }

}
