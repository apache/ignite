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

package org.apache.ignite.schema.parser;

import javafx.collections.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.schema.model.*;

import java.math.*;
import java.net.*;
import java.sql.*;
import java.util.*;

import static java.sql.Types.*;

/**
 * Database metadata parser.
 */
public class DatabaseMetadataParser {
    /**
     * @param name Source name.
     * @return String converted to java class name notation.
     */
    private static String toJavaClassName(String name) {
        int len = name.length();

        StringBuilder buf = new StringBuilder(len);

        boolean capitalizeNext = true;

        for (int i = 0; i < len; i++) {
            char ch = name.charAt(i);

            if (Character.isWhitespace(ch) || '_' == ch)
                capitalizeNext = true;
            else if (capitalizeNext) {
                buf.append(Character.toUpperCase(ch));

                capitalizeNext = false;
            }
            else
                buf.append(Character.toLowerCase(ch));
        }

        return buf.toString();
    }

    /**
     * @param name Source name.
     * @return String converted to java field name notation.
     */
    private static String toJavaFieldName(String name) {
        String javaName = toJavaClassName(name);

        return Character.toLowerCase(javaName.charAt(0)) + javaName.substring(1);
    }

    /**
     * Convert JDBC data type to java type.
     *
     * @param type JDBC SQL data type.
     * @param nullable {@code true} if {@code NULL} is allowed for this field in database.
     * @return Java data type.
     */
    private static Class<?> dataType(int type, boolean nullable) {
        switch (type) {
            case BIT:
            case BOOLEAN:
                return nullable ? Boolean.class : boolean.class;

            case TINYINT:
                return nullable ? Byte.class : byte.class;

            case SMALLINT:
                return nullable ? Short.class : short.class;

            case INTEGER:
                return nullable ? Integer.class : int.class;

            case BIGINT:
                return nullable ? Long.class : int.class;

            case REAL:
                return nullable ? Float.class : float.class;

            case FLOAT:
            case DOUBLE:
                return nullable ? Double.class : double.class;

            case NUMERIC:
            case DECIMAL:
                return BigDecimal.class;

            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
                return String.class;

            case DATE:
                return java.sql.Date.class;

            case TIME:
                return java.sql.Time.class;

            case TIMESTAMP:
                return java.sql.Timestamp.class;

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
            case CLOB:
            case NCLOB:
                return java.lang.reflect.Array.class;

            case NULL:
                return Void.class;

            case DATALINK:
                return URL.class;

            // OTHER, JAVA_OBJECT, DISTINCT, STRUCT, REF, ROWID, SQLXML
            default:
                return Object.class;
        }
    }

    /**
     * Parse database metadata.
     *
     * @param dbMeta Database metadata.
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param tbl Table name.
     * @return New initialized instance of {@code CacheQueryTypeMetadata}.
     * @throws SQLException If parsing failed.
     */
    private static PojoDescriptor parseTable(PojoDescriptor parent, DatabaseMetaData dbMeta, String catalog,
        String schema, String tbl) throws SQLException {
        CacheQueryTypeMetadata typeMeta = new CacheQueryTypeMetadata();

        typeMeta.setSchema(schema);
        typeMeta.setTableName(tbl);

        typeMeta.setType(toJavaClassName(tbl));
        typeMeta.setKeyType(typeMeta.getType() + "Key");

        Collection<CacheQueryTypeDescriptor> keyDescs = typeMeta.getKeyDescriptors();
        Collection<CacheQueryTypeDescriptor> valDescs = typeMeta.getValueDescriptors();

        Map<String, Class<?>> qryFields = typeMeta.getQueryFields();
        Map<String, Class<?>> ascFields = typeMeta.getAscendingFields();
        Map<String, Class<?>> descFields = typeMeta.getDescendingFields();
        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> groups = typeMeta.getGroups();

        Set<String> pkFlds = new LinkedHashSet<>();

        try (ResultSet pk = dbMeta.getPrimaryKeys(catalog, schema, tbl)) {
            while (pk.next())
                pkFlds.add(pk.getString(4));
        }

        List<PojoField> fields = new ArrayList<>();

        try (ResultSet cols = dbMeta.getColumns(catalog, schema, tbl, null)) {
            while (cols.next()) {
                String dbName = cols.getString(4);

                int dbType = cols.getInt(5);

                boolean nullable = cols.getInt(11) == DatabaseMetaData.columnNullable;

                String javaName = toJavaFieldName(dbName);

                Class<?> javaType = dataType(dbType, nullable);

                CacheQueryTypeDescriptor desc = new CacheQueryTypeDescriptor(javaName, javaType, dbName, dbType);

                boolean key = pkFlds.contains(dbName);

                if (key)
                    keyDescs.add(desc);
                else
                    valDescs.add(desc);

                qryFields.put(javaName, javaType);

                fields.add(new PojoField(key, desc, nullable));
            }
        }

        try (ResultSet idxs = dbMeta.getIndexInfo(catalog, schema, tbl, false, true)) {
            while (idxs.next()) {
                String idx = toJavaFieldName(idxs.getString(6));
                String col = toJavaFieldName(idxs.getString(9));
                String askOrDesc = idxs.getString(10);

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxCols = groups.get(idx);

                if (idxCols == null) {
                    idxCols = new LinkedHashMap<>();

                    groups.put(idx, idxCols);
                }

                Class<?> dataType = qryFields.get(col);

                Boolean desc = askOrDesc != null ? "D".equals(askOrDesc) : null;

                if (desc != null) {
                    if (desc)
                        descFields.put(col, dataType);
                    else
                        ascFields.put(col, dataType);
                }

                idxCols.put(col, new IgniteBiTuple<Class<?>, Boolean>(dataType, desc));
            }
        }

        return new PojoDescriptor(parent, typeMeta, fields);
    }

    /**
     * Parse database metadata.
     *
     * @param conn Connection to database.
     * @return Map with schemes and tables metadata.
     * @throws SQLException If parsing failed.
     */
    public static ObservableList<PojoDescriptor> parse(Connection conn) throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();

        List<PojoDescriptor> res = new ArrayList<>();

        try (ResultSet schemas = dbMeta.getSchemas()) {
            while (schemas.next()) {
                String schema = schemas.getString(1);

                // Skip system tables from INFORMATION_SCHEMA.
                if ("INFORMATION_SCHEMA".equalsIgnoreCase(schema))
                    continue;

                String catalog = schemas.getString(2);

                PojoDescriptor parent = PojoDescriptor.schema(schema);

                List<PojoDescriptor> children = new ArrayList<>();

                try (ResultSet tbls = dbMeta.getTables(catalog, schema, "%", null)) {
                    while (tbls.next()) {
                        String tbl = tbls.getString(3);

                        children.add(parseTable(parent, dbMeta, catalog, schema, tbl));
                    }
                }

                if (!children.isEmpty()) {
                    parent.children(children);

                    res.add(parent);
                    res.addAll(children);
                }
            }
        }

        Collections.sort(res, new Comparator<PojoDescriptor>() {
            @Override public int compare(PojoDescriptor o1, PojoDescriptor o2) {
                return o1.fullDbName().compareTo(o2.fullDbName());
            }
        });

        return FXCollections.observableList(res);
    }
}
