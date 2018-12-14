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

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VER_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.UPDATE_RESULT_META;

/**
 * H2 utility methods.
 */
public class H2Utils {
    /** Spatial index class name. */
    private static final String SPATIAL_IDX_CLS =
        "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex";

    /** Quotation character. */
    private static final char ESC_CH = '\"';

    /**
     * @param c1 First column.
     * @param c2 Second column.
     * @return {@code true} If they are the same.
     */
    public static boolean equals(IndexColumn c1, IndexColumn c2) {
        return c1.column.getColumnId() == c2.column.getColumnId();
    }

    /**
     * @param cols Columns list.
     * @param col Column to find.
     * @return {@code true} If found.
     */
    public static boolean containsColumn(List<IndexColumn> cols, IndexColumn col) {
        for (int i = cols.size() - 1; i >= 0; i--) {
            if (equals(cols.get(i), col))
                return true;
        }

        return false;
    }

    /**
     * Check whether columns list contains key or key alias column.
     *
     * @param desc Row descriptor.
     * @param cols Columns list.
     * @return Result.
     */
    public static boolean containsKeyColumn(GridH2RowDescriptor desc, List<IndexColumn> cols) {
        for (int i = cols.size() - 1; i >= 0; i--) {
            if (desc.isKeyColumn(cols.get(i).column.getColumnId()))
                return true;
        }

        return false;
    }

    /**
     * Prepare SQL statement for CREATE TABLE command.
     *
     * @param tbl Table descriptor.
     * @return SQL.
     */
    public static String tableCreateSql(H2TableDescriptor tbl) {
        GridQueryProperty keyProp = tbl.type().property(KEY_FIELD_NAME);
        GridQueryProperty valProp = tbl.type().property(VAL_FIELD_NAME);

        String keyType = dbTypeFromClass(tbl.type().keyClass(),
            keyProp == null ? -1 : keyProp.precision(),
            keyProp == null ? -1 : keyProp.scale());

        String valTypeStr = dbTypeFromClass(tbl.type().valueClass(),
            valProp == null ? -1 : valProp.precision(),
            valProp == null ? -1 : valProp.scale());

        SB sql = new SB();

        String keyValVisibility = tbl.type().fields().isEmpty() ? " VISIBLE" : " INVISIBLE";

        sql.a("CREATE TABLE ").a(tbl.fullTableName()).a(" (")
            .a(KEY_FIELD_NAME).a(' ').a(keyType).a(keyValVisibility).a(" NOT NULL");

        sql.a(',').a(VAL_FIELD_NAME).a(' ').a(valTypeStr).a(keyValVisibility);
        sql.a(',').a(VER_FIELD_NAME).a(" OTHER INVISIBLE");

        for (Map.Entry<String, Class<?>> e : tbl.type().fields().entrySet()) {
            GridQueryProperty prop = tbl.type().property(e.getKey());

            sql.a(',')
                .a(withQuotes(e.getKey()))
                .a(' ')
                .a(dbTypeFromClass(e.getValue(), prop.precision(), prop.scale()))
                .a(prop.notNull() ? " NOT NULL" : "");
        }

        sql.a(')');

        return sql.toString();
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param fullTblName Fully qualified table name.
     * @param h2Idx H2 index.
     * @param ifNotExists Quietly skip index creation if it exists.
     * @return Statement string.
     */
    public static String indexCreateSql(String fullTblName, GridH2IndexBase h2Idx, boolean ifNotExists) {
        boolean spatial = F.eq(SPATIAL_IDX_CLS, h2Idx.getClass().getName());

        GridStringBuilder sb = new SB("CREATE ")
            .a(spatial ? "SPATIAL " : "")
            .a("INDEX ")
            .a(ifNotExists ? "IF NOT EXISTS " : "")
            .a(withQuotes(h2Idx.getName()))
            .a(" ON ")
            .a(fullTblName)
            .a(" (");

        boolean first = true;

        for (IndexColumn col : h2Idx.getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a(withQuotes(col.columnName)).a(" ").a(col.sortType == SortOrder.ASCENDING ? "ASC" : "DESC");
        }

        sb.a(')');

        return sb.toString();
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param schemaName <b>Quoted</b> schema name.
     * @param idxName Index name.
     * @param ifExists Quietly skip index drop if it exists.
     * @return Statement string.
     */
    public static String indexDropSql(String schemaName, String idxName, boolean ifExists) {
        return "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + withQuotes(schemaName) + '.' + withQuotes(idxName);
    }

    /**
     * @param desc Row descriptor.
     * @param cols Columns list.
     * @param keyCol Primary key column.
     * @param affCol Affinity key column.
     * @return The same list back.
     */
    public static List<IndexColumn> treeIndexColumns(GridH2RowDescriptor desc, List<IndexColumn> cols,
        IndexColumn keyCol, IndexColumn affCol) {
        assert keyCol != null;

        if (!containsKeyColumn(desc, cols))
            cols.add(keyCol);

        if (affCol != null && !containsColumn(cols, affCol))
            cols.add(affCol);

        return cols;
    }

    /**
     * Create spatial index.
     *
     * @param tbl Table.
     * @param idxName Index name.
     * @param cols Columns.
     */
    public static GridH2IndexBase createSpatialIndex(GridH2Table tbl, String idxName, IndexColumn[] cols) {
        try {
            Class<?> cls = Class.forName(SPATIAL_IDX_CLS);

            Constructor<?> ctor = cls.getConstructor(
                GridH2Table.class,
                String.class,
                Integer.TYPE,
                IndexColumn[].class);

            if (!ctor.isAccessible())
                ctor.setAccessible(true);

            final int segments = tbl.rowDescriptor().context().config().getQueryParallelism();

            return (GridH2IndexBase)ctor.newInstance(tbl, idxName, segments, cols);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: " + SPATIAL_IDX_CLS, e);
        }
    }

    /**
     * Add quotes around the name.
     *
     * @param str String.
     * @return String with quotes.
     */
    public static String withQuotes(String str) {
        return ESC_CH + str + ESC_CH;
    }

    /**
     * @param rsMeta Metadata.
     * @return List of fields metadata.
     * @throws SQLException If failed.
     */
    public static List<GridQueryFieldMetadata> meta(ResultSetMetaData rsMeta) throws SQLException {
        List<GridQueryFieldMetadata> meta = new ArrayList<>(rsMeta.getColumnCount());

        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
            String schemaName = rsMeta.getSchemaName(i);
            String typeName = rsMeta.getTableName(i);
            String name = rsMeta.getColumnLabel(i);
            String type = rsMeta.getColumnClassName(i);
            int precision = rsMeta.getPrecision(i);
            int scale = rsMeta.getScale(i);

            if (type == null) // Expression always returns NULL.
                type = Void.class.getName();

            meta.add(new H2SqlFieldMetadata(schemaName, typeName, name, type, precision, scale));
        }

        return meta;
    }

    /**
     * @param c Connection.
     * @return Session.
     */
    public static Session session(Connection c) {
        return (Session)((JdbcConnection)c).getSession();
    }

    /**
     * @param conn Connection to use.
     * @param distributedJoins If distributed joins are enabled.
     * @param enforceJoinOrder Enforce join order of tables.
     */
    public static void setupConnection(Connection conn, boolean distributedJoins, boolean enforceJoinOrder) {
        Session s = session(conn);

        s.setForceJoinOrder(enforceJoinOrder);
        s.setJoinBatchEnabled(distributedJoins);
    }

    /**
     * Convert value to column's expected type by means of H2.
     *
     * @param val Source value.
     * @param idx Row descriptor.
     * @param type Expected column type to convert to.
     * @return Converted object.
     * @throws IgniteCheckedException if failed.
     */
    public static Object convert(Object val, IgniteH2Indexing idx, int type) throws IgniteCheckedException {
        if (val == null)
            return null;

        int objType = DataType.getTypeFromClass(val.getClass());

        if (objType == type)
            return val;

        Value h2Val = wrap(idx.objectContext(), val, objType);

        return h2Val.convertTo(type).getObject();
    }

    /**
     * Private constructor.
     */
    private H2Utils() {
        // No-op.
    }

    /**
     * @return Single-column, single-row cursor with 0 as number of updated records.
     */
    @SuppressWarnings("unchecked")
    public static QueryCursorImpl<List<?>> zeroCursor() {
        QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
            (Collections.singletonList(0L)), null, false);

        resCur.fieldsMeta(UPDATE_RESULT_META);

        return resCur;
    }

    /**
     * Add only new columns to destination list.
     *
     * @param dest List of index columns to add new elements from src list.
     * @param src List of IndexColumns to add to dest list.
     */
    public static void addUniqueColumns(List<IndexColumn> dest, List<IndexColumn> src) {
        for (IndexColumn col : src) {
            if (!containsColumn(dest, col))
                dest.add(col);
        }
    }

    /**
     * Check that given table has not started cache and start it for such case.
     *
     * @param tbl Table to check on not started cache.
     * @return {@code true} in case not started and has been started.
     */
    public static boolean checkAndStartNotStartedCache(GridH2Table tbl) {
        if (tbl != null && tbl.isCacheLazy()) {
            String cacheName = tbl.cacheInfo().config().getName();

            GridKernalContext ctx = tbl.cacheInfo().context();

            try {
                Boolean res = ctx.cache().dynamicStartCache(null, cacheName, null, false, true, true).get();

                return U.firstNotNull(res, Boolean.FALSE);
            }
            catch (IgniteCheckedException ex) {
                throw U.convertException(ex);
            }
        }

        return false;
    }

    /**
     * Wraps object to respective {@link Value}.
     *
     * @param obj Object.
     * @param type Value type.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public static Value wrap(CacheObjectValueContext coCtx, Object obj, int type) throws IgniteCheckedException {
        assert obj != null;

        if (obj instanceof CacheObject) { // Handle cache object.
            CacheObject co = (CacheObject)obj;

            if (type == Value.JAVA_OBJECT)
                return new GridH2ValueCacheObject(co, coCtx);

            obj = co.value(coCtx, false);
        }

        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get((Boolean)obj);
            case Value.BYTE:
                return ValueByte.get((Byte)obj);
            case Value.SHORT:
                return ValueShort.get((Short)obj);
            case Value.INT:
                return ValueInt.get((Integer)obj);
            case Value.FLOAT:
                return ValueFloat.get((Float)obj);
            case Value.LONG:
                return ValueLong.get((Long)obj);
            case Value.DOUBLE:
                return ValueDouble.get((Double)obj);
            case Value.UUID:
                UUID uuid = (UUID)obj;
                return ValueUuid.get(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            case Value.DATE:
                if (LocalDateTimeUtils.LOCAL_DATE == obj.getClass())
                    return LocalDateTimeUtils.localDateToDateValue(obj);

                return ValueDate.get((Date)obj);

            case Value.TIME:
                if (LocalDateTimeUtils.LOCAL_TIME == obj.getClass())
                    return LocalDateTimeUtils.localTimeToTimeValue(obj);

                return ValueTime.get((Time)obj);

            case Value.TIMESTAMP:
                if (obj instanceof java.util.Date && !(obj instanceof Timestamp))
                    obj = new Timestamp(((java.util.Date)obj).getTime());

                if (LocalDateTimeUtils.LOCAL_DATE_TIME == obj.getClass())
                    return LocalDateTimeUtils.localDateTimeToValue(obj);

                return ValueTimestamp.get((Timestamp)obj);

            case Value.DECIMAL:
                return ValueDecimal.get((BigDecimal)obj);
            case Value.STRING:
                return ValueString.get(obj.toString());
            case Value.BYTES:
                return ValueBytes.get((byte[])obj);
            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(obj, null, null);
            case Value.ARRAY:
                Object[] arr = (Object[])obj;

                Value[] valArr = new Value[arr.length];

                for (int i = 0; i < arr.length; i++) {
                    Object o = arr[i];

                    valArr[i] = o == null ? ValueNull.INSTANCE :
                        wrap(coCtx, o, DataType.getTypeFromClass(o.getClass()));
                }

                return ValueArray.get(valArr);

            case Value.GEOMETRY:
                return ValueGeometry.getFromGeometry(obj);
        }

        throw new IgniteCheckedException("Failed to wrap value[type=" + type + ", value=" + obj + "]");
    }

    /**
     * Validates properties described by query types.
     *
     * @param type Type descriptor.
     * @throws IgniteCheckedException If validation failed.
     */
    @SuppressWarnings("CollectionAddAllCanBeReplacedWithConstructor")
    public static void validateTypeDescriptor(GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        assert type != null;

        Collection<String> names = new HashSet<>();

        names.addAll(type.fields().keySet());

        if (names.size() < type.fields().size())
            throw new IgniteCheckedException("Found duplicated properties with the same name [keyType=" +
                type.keyClass().getName() + ", valueType=" + type.valueClass().getName() + "]");

        String ptrn = "Name ''{0}'' is reserved and cannot be used as a field name [type=" + type.name() + "]";

        for (String name : names) {
            if (name.equalsIgnoreCase(KEY_FIELD_NAME) ||
                name.equalsIgnoreCase(VAL_FIELD_NAME) ||
                name.equalsIgnoreCase(VER_FIELD_NAME))
                throw new IgniteCheckedException(MessageFormat.format(ptrn, name));
        }
    }

    /**
     * Gets corresponding DB type from java class.
     *
     * @param cls Java class.
     * @param precision Field precision.
     * @param scale Field scale.
     * @return DB type name.
     */
    private static String dbTypeFromClass(Class<?> cls, int precision, int scale) {
        String dbType = H2DatabaseType.fromClass(cls).dBTypeAsString();

        if (precision != -1 && dbType.equalsIgnoreCase(H2DatabaseType.VARCHAR.dBTypeAsString()))
            return dbType + "(" + precision + ")";

        return dbType;
    }

    /**
     * Generate SqlFieldsQuery string from SqlQuery.
     *
     * @param qry Query string.
     * @param tableAlias table alias.
     * @param tbl Table to use.
     * @return Prepared statement.
     * @throws IgniteCheckedException In case of error.
     */
    public static String generateFieldsQueryString(String qry, String tableAlias, H2TableDescriptor tbl)
        throws IgniteCheckedException {
        assert tbl != null;

        final String qry0 = qry;

        String t = tbl.fullTableName();

        String from = " ";

        qry = qry.trim();

        String upper = qry.toUpperCase();

        if (upper.startsWith("SELECT")) {
            qry = qry.substring(6).trim();

            final int star = qry.indexOf('*');

            if (star == 0)
                qry = qry.substring(1).trim();
            else if (star > 0) {
                if (F.eq('.', qry.charAt(star - 1))) {
                    t = qry.substring(0, star - 1);

                    qry = qry.substring(star + 1).trim();
                }
                else
                    throw new IgniteCheckedException("Invalid query (missing alias before asterisk): " + qry0);
            }
            else
                throw new IgniteCheckedException("Only queries starting with 'SELECT *' and 'SELECT alias.*' " +
                    "are supported (rewrite your query or use SqlFieldsQuery instead): " + qry0);

            upper = qry.toUpperCase();
        }

        if (!upper.startsWith("FROM"))
            from = " FROM " + t + (tableAlias != null ? " as " + tableAlias : "") +
                (upper.startsWith("WHERE") || upper.startsWith("ORDER") || upper.startsWith("LIMIT") ?
                    " " : " WHERE ");

        if(tableAlias != null)
            t = tableAlias;

        qry = "SELECT " + t + "." + KEY_FIELD_NAME + ", " + t + "." + VAL_FIELD_NAME + from + qry;

        return qry;
    }
}
