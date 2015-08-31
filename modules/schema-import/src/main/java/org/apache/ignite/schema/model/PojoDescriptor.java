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

package org.apache.ignite.schema.model;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.apache.ignite.schema.parser.DbColumn;
import org.apache.ignite.schema.parser.DbTable;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BIT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

/**
 * Descriptor for java type.
 */
public class PojoDescriptor {
    /** Database table. */
    private final DbTable tbl;

    /** Selected property. */
    private final BooleanProperty useProp;

    /** Previous name for key class. */
    private final String keyClsNamePrev;

    /** Key class name to show on screen. */
    private final StringProperty keyClsNameProp;

    /** Previous name for value class. */
    private final String valClsNamePrev;

    /** Value class name to show on screen. */
    private final StringProperty valClsNameProp;

    /** Parent item (schema name). */
    private final PojoDescriptor parent;

    /** Children items (tables names). */
    private Collection<PojoDescriptor> children = Collections.emptyList();

    /** Indeterminate state of parent. */
    private final BooleanProperty indeterminateProp = new SimpleBooleanProperty(false);

    /** Full database name: schema + table. */
    private final String fullDbName;

    /** Java class fields. */
    private final ObservableList<PojoField> fields;

    /** Fields map for quick access. */
    private final Map<String, PojoField> fieldsMap;

    /**
     * Constructor of POJO descriptor.
     *
     * @param prn Parent descriptor.
     * @param tbl Database table Tab;e.
     */
    public PojoDescriptor(PojoDescriptor prn, DbTable tbl) {
        parent = prn;

        this.tbl = tbl;

        fullDbName = tbl.schema() + "." + tbl.table();

        valClsNamePrev = toJavaClassName(tbl.table());
        valClsNameProp = new SimpleStringProperty(valClsNamePrev);

        keyClsNamePrev = valClsNamePrev.isEmpty() ? "" : valClsNamePrev + "Key";
        keyClsNameProp = new SimpleStringProperty(keyClsNamePrev);

        Collection<DbColumn> cols = tbl.columns();

        List<PojoField> flds = new ArrayList<>(cols.size());

        fieldsMap = new HashMap<>(cols.size());

        for (DbColumn col : cols) {
            String colName = col.name();

            PojoField fld = new PojoField(colName, col.type(),
                toJavaFieldName(colName), toJavaType(col.type(), col.nullable()).getName(),
                col.key(), col.nullable());

            fld.owner(this);

            flds.add(fld);

            fieldsMap.put(colName, fld);
        }

        fields = FXCollections.observableList(flds);

        boolean isTbl = parent != null;

        boolean hasKeys = !isTbl || !keyFields().isEmpty();

        useProp = new SimpleBooleanProperty(hasKeys);

        if (isTbl && !hasKeys && !parent.indeterminateProp.get())
            parent.indeterminateProp.set(true);

        useProp.addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> val, Boolean oldVal, Boolean newVal) {
                for (PojoDescriptor child : children)
                    child.useProp.set(newVal);

                if (parent != null && !parent.children.isEmpty()) {
                    Iterator<PojoDescriptor> it = parent.children.iterator();

                    boolean parentIndeterminate = false;
                    boolean first = it.next().useProp.get();

                    while (it.hasNext()) {
                        if (it.next().useProp.get() != first) {
                            parentIndeterminate = true;

                            break;
                        }
                    }

                    parent.indeterminateProp.set(parentIndeterminate);

                    if (!parentIndeterminate)
                        parent.useProp.set(first);
                }
            }
        });
    }

    /**
     * @return Parent descriptor.
     */
    public PojoDescriptor parent() {
        return parent;
    }

    /**
     * @return Full database name: schema + table.
     */
    public String fullDbName() {
        return fullDbName;
    }

    /**
     * @return {@code true} if POJO descriptor is a table descriptor and checked in GUI.
     */
    public boolean checked() {
        return parent != null && useProp.get();
    }

    /**
     * @return Boolean property support for {@code use} property.
     */
    public BooleanProperty useProperty() {
        return useProp;
    }

    /**
     * @return Boolean property support for parent {@code indeterminate} property.
     */
    public BooleanProperty indeterminate() {
        return indeterminateProp;
    }

    /**
     * @return Key class name.
     */
    public String keyClassName() {
        return keyClsNameProp.get();
    }

    /**
     * @param name New key class name.
     */
    public void keyClassName(String name) {
        keyClsNameProp.set(name);
    }

    /**
     * @return Value class name.
     */
    public String valueClassName() {
        return valClsNameProp.get();
    }

    /**
     * @param name New value class name.
     */
    public void valueClassName(String name) {
        valClsNameProp.set(name);
    }

    /**
     * @return {@code true} if at least one field checked as &quot;used&quot;.
     */
    public boolean hasFields() {
        for (PojoField field : fields)
            if (field.use())
                return true;

        return false;
    }

    /**
     * @return {@code true} if at least one field checked as &quot;used&quot; and checked as &quot;key&quot;.
     */
    public boolean hasKeyFields() {
        for (PojoField field : fields)
            if (field.use() && field.key())
                return true;

        return false;
    }

    /**
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @return {@code true} if at least one field checked as &quot;used&quot; and not checked as &quot;key&quot;.
     */
    public boolean hasValueFields(boolean includeKeys) {
        if (includeKeys)
            return hasKeyFields();

        for (PojoField field : fields)
            if (field.use() && !field.key())
                return true;

        return false;
    }

    /**
     * @return Collection of key fields.
     */
    public Collection<PojoField> keyFields() {
        Collection<PojoField> keys = new ArrayList<>();

        for (PojoField field : fields)
            if (field.use() && field.key() )
                keys.add(field);

        return keys;
    }

    /**
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @return Collection of value fields.
     */
    public Collection<PojoField> valueFields(boolean includeKeys) {
        Collection<PojoField> vals = new ArrayList<>();

        for (PojoField field : fields)
            if (field.use() && (includeKeys || !field.key()))
                vals.add(field);

        return vals;
    }

    /**
     * @return Ascending fields.
     */
    public Collection<PojoField> ascendingFields() {
        Collection<PojoField> res = new ArrayList<>();

        Set<String> asc = tbl.ascendingColumns();

        for (PojoField field : fields)
            if (field.use() && asc.contains(field.dbName()))
                res.add(field);

        return res;
    }

    /**
     * @return Descending fields.
     */
    public Collection<PojoField> descendingFields() {
        Collection<PojoField> res = new ArrayList<>();

        Set<String> desc = tbl.descendingColumns();

        for (PojoField field : fields)
            if (field.use() && desc.contains(field.dbName()))
                res.add(field);

        return res;
    }

    /**
     * Gets indexes groups.
     *
     * @return Map with indexes.
     */
    public Map<String, Map<String, IndexItem>> groups() {
        Map<String, Map<String, Boolean>> idxs = tbl.indexes();

        Map<String, Map<String, IndexItem>> groups = new LinkedHashMap<>(idxs.size());

        for (Map.Entry<String, Map<String, Boolean>> idx : idxs.entrySet()) {
            Map<String, Boolean> idxCols = idx.getValue();

            if (idxCols.size() > 1) {
                String idxName = idx.getKey();

                Map<String, IndexItem> grp = new LinkedHashMap<>();

                groups.put(idxName, grp);

                for (Map.Entry<String, Boolean> idxCol : idxCols.entrySet()) {
                    PojoField fld = fieldsMap.get(idxCol.getKey());

                    grp.put(fld.javaName(), new IndexItem(fld.javaTypeName(), idxCol.getValue()));
                }
            }
        }

        return groups;
    }

    /**
     * @return Key class name property.
     */
    public StringProperty keyClassNameProperty() {
        return keyClsNameProp;
    }

    /**
     * @return Value class name property.
     */
    public StringProperty valueClassNameProperty() {
        return valClsNameProp;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return tbl.schema();
    }

    /**
     * @return Table name.
     */
    public String table() {
        return tbl.table();
    }

    /**
     * Sets children items.
     *
     * @param children Items to set.
     */
    public void children(Collection<PojoDescriptor> children) {
        this.children = children;
    }

    /**
     * @return {@code true} if descriptor was changed by user via GUI.
     */
    public boolean changed() {
        if (!keyClsNameProp.get().equals(keyClsNamePrev) || !valClsNameProp.get().equals(valClsNamePrev))
            return true;

        for (PojoField field : fields)
            if (field.changed())
                return true;

        return false;
    }

    /**
     * Revert changes to key class name made by user.
     */
    public void revertKeyClassName() {
        keyClsNameProp.set(keyClsNamePrev);
    }

    /**
     * Revert changes to value class name made by user.
     */
    public void revertValueClassName() {
        valClsNameProp.set(valClsNamePrev);
    }

    /**
     * Revert changes to java names made by user.
     */
    public void revertJavaNames() {
        for (PojoField field : fields)
            field.resetJavaName();
    }

    /**
     * @return Java class fields.
     */
    public ObservableList<PojoField> fields() {
        return fields;
    }

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
    private static Class<?> toJavaType(int type, boolean nullable) {
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
                return nullable ? Long.class : long.class;

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
            case CLOB:
            case NCLOB:
                return String.class;

            case DATE:
                return java.sql.Date.class;

            case TIME:
                return java.sql.Time.class;

            case TIMESTAMP:
                return java.sql.Timestamp.class;

            // BINARY, VARBINARY, LONGVARBINARY, ARRAY, BLOB, NULL, DATALINK
            // OTHER, JAVA_OBJECT, DISTINCT, STRUCT, REF, ROWID, SQLXML
            default:
                return Object.class;
        }
    }
}