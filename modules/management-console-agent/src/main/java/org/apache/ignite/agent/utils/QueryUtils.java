/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.utils;

import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.agent.action.query.CursorHolder;
import org.apache.ignite.agent.dto.action.query.QueryArgument;
import org.apache.ignite.agent.dto.action.query.QueryField;
import org.apache.ignite.agent.dto.action.query.QueryResult;
import org.apache.ignite.agent.dto.cache.CacheSqlIndexMetadata;
import org.apache.ignite.agent.dto.cache.CacheSqlMetadata;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * SQL query utils.
 */
public class QueryUtils {
    /** Columns for SCAN queries. */
    public static final List<QueryField> SCAN_COL_NAMES = Arrays.asList(
        new QueryField().setFieldName("Key Class"), new QueryField().setFieldName("Key"),
        new QueryField().setFieldName("Value Class"), new QueryField().setFieldName("Value")
    );

    /**
     * @param curHolder Cursor id.
     * @param pageSize Page size.
     * @return Query result.
     */
    public static QueryResult fetchResult(CursorHolder curHolder, int pageSize) {
        return curHolder.scanCursor()
            ? fetchScanQueryResult(curHolder, pageSize)
            : fetchSqlQueryResult(curHolder, pageSize);
    }

    /**
     * @param curHolder Cursor id.
     * @param pageSize Page size.
     * @return Query result.
     */
    public static QueryResult fetchSqlQueryResult(CursorHolder curHolder, int pageSize) {
        QueryResult qryRes = new QueryResult();

        long start = U.currentTimeMillis();

        List<Object[]> rows = fetchSqlQueryRows(curHolder, pageSize);

        List<QueryField> cols = getColumns(curHolder.cursor());

        boolean hasMore = curHolder.hasNext();

        return qryRes
            .setHasMore(hasMore)
            .setColumns(cols)
            .setRows(rows)
            .setDuration(U.currentTimeMillis() - start);
    }

    /**
     * @param curHolder Cursor id.
     * @param pageSize Page size.
     * @return Query result.
     */
    public static QueryResult fetchScanQueryResult(CursorHolder curHolder, int pageSize) {
        QueryResult qryRes = new QueryResult();

        long start = U.currentTimeMillis();

        List<Object[]> rows = fetchScanQueryRows(curHolder, pageSize);

        boolean hasMore = curHolder.hasNext();

        return qryRes
                .setHasMore(hasMore)
                .setColumns(SCAN_COL_NAMES)
                .setRows(rows)
                .setDuration(U.currentTimeMillis() - start);
    }

    /**
     * @param cursor Query cursor.
     * @return List of columns.
     */
    public static List<QueryField> getColumns(QueryCursor cursor) {
        List<GridQueryFieldMetadata> meta = ((QueryCursorEx)cursor).fieldsMeta();

        if (meta == null)
            return Collections.emptyList();

        List<QueryField> res = new ArrayList<>(meta.size());

        for (GridQueryFieldMetadata col : meta) {
            res.add(
                new QueryField()
                    .setSchemaName(col.schemaName())
                    .setTypeName(col.typeName())
                    .setFieldName(col.fieldName())
                    .setFieldTypeName(col.fieldTypeName())
            );
        }

        return res;
    }

    /**
     * @param arg Argument.
     * @return Prepared query.
     */
    public static SqlFieldsQuery prepareQuery(QueryArgument arg) {
        SqlFieldsQuery qry = new SqlFieldsQuery(arg.getQueryText());

        qry.setPageSize(arg.getPageSize());
        qry.setLocal(arg.getTargetNodeId() != null);
        qry.setDistributedJoins(arg.isDistributedJoins());
        qry.setCollocated(arg.isCollocated());
        qry.setEnforceJoinOrder(arg.isEnforceJoinOrder());
        qry.setLazy(arg.isLazy());

        if (!F.isEmpty(arg.getCacheName()))
            qry.setSchema(arg.getCacheName());

        if (!F.isEmpty(arg.getParameters()))
            qry.setArgs(arg.getParameters());

        return qry;
    }

    /**
     * Collects rows from sql query future, first time creates meta and column names arrays.
     *
     * @param itr Result set iterator.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchSqlQueryRows(Iterator itr, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        Iterator<List<?>> sqlItr = (Iterator<List<?>>)itr;

        while (sqlItr.hasNext() && cnt < pageSize) {
            List<?> next = sqlItr.next();

            int sz = next.size();

            Object[] row = new Object[sz];

            for (int i = 0; i < sz; i++)
                row[i] = convertValue(next.get(i));

            rows.add(row);

            cnt++;
        }

        return rows;
    }
    /**
     * Convert object that can be passed to client.
     *
     * @param original Source object.
     * @return Converted value.
     */
    public static Object convertValue(Object original) {
        if (original == null)
            return null;
        else if (isKnownType(original))
            return original;
        else if (original instanceof BinaryObject)
            return binaryToString((BinaryObject)original);
        else
            return original.getClass().isArray() ? "binary" : original.toString();
    }

    /**
     * Checks is given object is one of known types.
     *
     * @param obj Object instance to check.
     * @return {@code true} if it is one of known types.
     */
    private static boolean isKnownType(Object obj) {
        return obj instanceof String ||
            obj instanceof Boolean ||
            obj instanceof Byte ||
            obj instanceof Integer ||
            obj instanceof Long ||
            obj instanceof Short ||
            obj instanceof Date ||
            obj instanceof Double ||
            obj instanceof Float ||
            obj instanceof BigDecimal ||
            obj instanceof URL;
    }

    /**
     * Fetch rows from SCAN query future.
     *
     * @param itr Result set iterator.
     * @param pageSize Number of rows to fetch.
     * @return Fetched rows.
     */
    public static List<Object[]> fetchScanQueryRows(Iterator itr, int pageSize) {
        List<Object[]> rows = new ArrayList<>();

        int cnt = 0;

        Iterator<Cache.Entry<Object, Object>> scanItr = (Iterator<Cache.Entry<Object, Object>>)itr;

        while (scanItr.hasNext() && cnt < pageSize) {
            Cache.Entry<Object, Object> next = scanItr.next();

            Object k = next.getKey();

            Object v = next.getValue();

            rows.add(new Object[] {typeOf(k), valueOf(k), typeOf(v), valueOf(v)});

            cnt++;
        }

        return rows;
    }

    /**
     * @param o Source object.
     * @return String representation of object class.
     */
    private static String typeOf(Object o) {
        if (o != null) {
            Class<?> clazz = o.getClass();

            return clazz.isArray() ? IgniteUtils.compact(clazz.getComponentType().getName()) + "[]"
                    : IgniteUtils.compact(o.getClass().getName());
        }
        else
            return "n/a";
    }

    /**
     * @param o Object.
     * @return String representation of value.
     */
    private static String valueOf(Object o) {
        if (o == null)
            return "null";

        if (o instanceof byte[])
            return "size=" + ((byte[])o).length;

        if (o instanceof Byte[])
            return "size=" + ((Byte[])o).length;

        if (o instanceof Object[])
            return "size=" + ((Object[])o).length + ", values=[" + mkString((Object[])o, 120) + ']';

        if (o instanceof BinaryObject)
            return binaryToString((BinaryObject)o);

        return o.toString();
    }

    /**
     * @param arr Object array.
     * @param maxSz Maximum string size.
     * @return Fixed size string.
     */
    private static String mkString(Object[] arr, int maxSz) {
        String sep = ", ";

        StringBuilder sb = new StringBuilder();

        boolean first = true;

        for (Object v : arr) {
            if (first)
                first = false;
            else
                sb.append(sep);

            sb.append(v);

            if (sb.length() > maxSz)
                break;
        }

        if (sb.length() >= maxSz) {
            String end = "...";

            sb.setLength(maxSz - end.length());

            sb.append(end);
        }

        return sb.toString();
    }

    /**
     * TODO GG-24424: Change on JSON string.
     * Convert Binary object to string.
     *
     * @param obj Binary object.
     * @return String representation of Binary object.
     */
    public static String binaryToString(BinaryObject obj) {
        int hash = obj.hashCode();

        if (obj instanceof BinaryObjectEx) {
            BinaryObjectEx objEx = (BinaryObjectEx)obj;

            BinaryType meta;

            try {
                meta = ((BinaryObjectEx)obj).rawType();
            }
            catch (BinaryObjectException ignore) {
                meta = null;
            }

            if (meta != null) {
                if (meta.isEnum()) {
                    try {
                        return obj.deserialize().toString();
                    }
                    catch (BinaryObjectException ignore) {
                        // NO-op.
                    }
                }

                SB buf = new SB(meta.typeName());

                if (meta.fieldNames() != null) {
                    buf.a(" [hash=").a(hash);

                    for (String name : meta.fieldNames()) {
                        Object val = objEx.field(name);

                        buf.a(", ").a(name).a('=').a(val);
                    }

                    buf.a(']');

                    return buf.toString();
                }
            }
        }

        return S.toString(obj.getClass().getSimpleName(),
                "hash", hash, false,
                "typeId", obj.type().typeId(), true);
    }

    /**
     * This code is copy-paste of GridCacheQuerySqlMetadataJobV2.
     *
     * @param cacheName Cache name.
     * @param types Types.
     * @return Cache sql metadata
     */
    public static List<CacheSqlMetadata> queryTypesToMetadataList(String cacheName, Collection<GridQueryTypeDescriptor> types) {
        List<CacheSqlMetadata> metadataList = new ArrayList<>();

        for (GridQueryTypeDescriptor type : types) {
            CacheSqlMetadata metadata = new CacheSqlMetadata().setCacheName(cacheName);

            // Filter internal types (e.g., data structures).
            if (type.name().startsWith("GridCache"))
                continue;

            metadata.setTypeName(type.name());
            metadata.setTableName(type.tableName());
            metadata.setSchemaName(type.schemaName());
            metadata.setKeyClass(type.keyClass().getName());
            metadata.setValueClass(type.valueClass().getName());
            metadata.setFields(getFields(type));
            metadata.setNotNullFields(getNotNullFields(type));
            metadata.setIndexes(getIndexes(type));

            metadataList.add(metadata);
        }

        return metadataList;
    }

    /**
     *
     * @param type Type.
     * @return List of cache sql index metadata.
     */
    private static List<CacheSqlIndexMetadata> getIndexes(GridQueryTypeDescriptor type) {
        List<CacheSqlIndexMetadata> indexes = new ArrayList<>();

        for (Map.Entry<String, GridQueryIndexDescriptor> e : type.indexes().entrySet()) {
            GridQueryIndexDescriptor desc = e.getValue();

            // Add only SQL indexes.
            if (desc.type() == QueryIndexType.SORTED) {
                Collection<String> idxFields = new ArrayList<>();

                Collection<String> descendings = new ArrayList<>();

                for (String idxField : e.getValue().fields()) {
                    String idxFieldUpper = idxField.toUpperCase();

                    idxFields.add(idxFieldUpper);

                    if (desc.descending(idxField))
                        descendings.add(idxFieldUpper);
                }

                indexes.add(
                    new CacheSqlIndexMetadata()
                        .setName(e.getKey().toUpperCase())
                        .setFields(idxFields)
                        .setUnique(false)
                        .setDescendings(descendings)
                );
            }
        }

        return indexes;
    }

    /**
     * @param type Type.
     * @return Set of not null fields.
     */
    private static Set<String> getNotNullFields(GridQueryTypeDescriptor type) {
        HashSet<String> notNullFieldsSet = new HashSet<>();

        for (Map.Entry<String, Class<?>> e : type.fields().entrySet()) {
            String fieldName = e.getKey();

            if (type.property(fieldName).notNull())
                notNullFieldsSet.add(fieldName.toUpperCase());
        }

        return notNullFieldsSet;
    }

    /**
     * @param type Type.
     * @return Map of field name and type.
     */
    private static Map<String, String> getFields(GridQueryTypeDescriptor type) {
        Map<String, String> fieldsMap = new LinkedHashMap<>();

        // _KEY and _VAL are not included in GridIndexingTypeDescriptor.valueFields
        if (type.fields().isEmpty()) {
            fieldsMap.put("_KEY", type.keyClass().getName());
            fieldsMap.put("_VAL", type.valueClass().getName());
        }

        for (Map.Entry<String, Class<?>> e : type.fields().entrySet()) {
            String fieldName = e.getKey();

            fieldsMap.put(fieldName.toUpperCase(), e.getValue().getName());
        }

        return fieldsMap;
    }
}
