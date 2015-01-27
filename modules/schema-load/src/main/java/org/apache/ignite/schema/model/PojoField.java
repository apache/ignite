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

import javafx.beans.property.*;
import javafx.collections.*;
import org.apache.ignite.cache.query.*;

import java.math.*;
import java.net.*;
import java.util.*;

import static java.sql.Types.*;

/**
 * Field descriptor with properties for JavaFX GUI bindings.
 */
public class PojoField {
    /** If this field belongs to primary key. */
    private final BooleanProperty key;

    /** If this field initially belongs to primary key. */
    private final boolean keyPrev;

    /** Field name for POJO. */
    private final StringProperty javaName;

    /** Initial field name for POJO. */
    private final String javaNamePrev;

    /** Field type for POJO. */
    private final StringProperty javaTypeName;

    /** Initial field type for POJO. */
    private final String javaTypeNamePrev;

    /** Field name in database. */
    private final StringProperty dbName;

    /** JDBC field type in database. */
    private final int dbType;

    /** Field type in database. */
    private final StringProperty dbTypeName;

    /** Is NULL allowed for field in database. */
    private final boolean nullable;

    /** List of possible java type conversions. */
    private final ObservableList<String> conversions;

    /** Field type descriptor. */
    private final CacheQueryTypeDescriptor desc;

    /**
     * @param clss List of classes to get class names.
     * @return List of classes names to show in UI for manual select.
     */
    private static List<String> classNames(Class<?>... clss) {
        List<String> names = new ArrayList<>(clss.length);

        for (Class<?> cls : clss)
            names.add(cls.getName());

        return names;
    }

    /** Null number conversions. */
    private static final ObservableList<String> NULL_NUM_CONVERSIONS = FXCollections.observableArrayList();

    /** Not null number conversions. */
    private static final ObservableList<String> NOT_NULL_NUM_CONVERSIONS = FXCollections.observableArrayList();

    /** Primitive types. */
    private static final List<String> PRIMITIVES = classNames(boolean.class, byte.class, short.class,
        int.class, long.class, float.class, double.class);

    /** Java types. */
    private static final Class<?>[] JAVA_TYPES = new Class<?>[] {
        boolean.class, Boolean.class,
        byte.class, Byte.class,
        short.class, Short.class,
        int.class, Integer.class,
        long.class, Long.class,
        float.class, Float.class,
        double.class, Double.class,
        BigDecimal.class,
        String.class,
        java.sql.Date.class, java.sql.Time.class, java.sql.Timestamp.class,
        java.lang.reflect.Array.class, Void.class, URL.class, Object.class};

    /** */
    private static final Map<String, Class<?>> classesMap = new HashMap<>();

    static {
        List<String> objects = classNames(Boolean.class, Byte.class, Short.class, Integer.class,
            Long.class, Float.class, Double.class, BigDecimal.class);

        NOT_NULL_NUM_CONVERSIONS.addAll(PRIMITIVES);
        NOT_NULL_NUM_CONVERSIONS.addAll(objects);

        NULL_NUM_CONVERSIONS.addAll(objects);

        for (Class<?> cls : JAVA_TYPES)
            classesMap.put(cls.getName(), cls);
    }

    /**
     * @param dbType Database type.
     * @param nullable Nullable.
     * @param dflt Default.
     * @return List of possible type conversions.
     */
    private static ObservableList<String> conversions(int dbType, boolean nullable, String dflt) {
        switch (dbType) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return nullable ? NULL_NUM_CONVERSIONS : NOT_NULL_NUM_CONVERSIONS;

            default:
                return FXCollections.singletonObservableList(dflt);
        }
    }

    /**
     * @param key {@code true} if this field belongs to primary key.
     * @param desc Field type descriptor.
     * @param nullable {@code true} if {@code NULL} is allowed for this field in database.
     */
    public PojoField(boolean key, CacheQueryTypeDescriptor desc, boolean nullable) {
        keyPrev = key;

        this.key = new SimpleBooleanProperty(key);

        javaNamePrev = desc.getJavaName();

        javaName = new SimpleStringProperty(javaNamePrev);

        javaTypeNamePrev = desc.getJavaType().getName();

        javaTypeName = new SimpleStringProperty(javaTypeNamePrev);

        dbName = new SimpleStringProperty(desc.getDbName());

        dbType = desc.getDbType();

        dbTypeName = new SimpleStringProperty(jdbcTypeName(dbType));

        this.nullable = nullable;

        conversions = conversions(dbType, nullable, javaNamePrev);

        this.desc = desc;
    }

    /**
     * @param jdbcType String name for JDBC type.
     */
    private static String jdbcTypeName(int jdbcType) {
        switch (jdbcType) {
            case BIT:
                return "BIT";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case REAL:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
            case NUMERIC:
                return "NUMERIC";
            case DECIMAL:
                return "DECIMAL";
            case CHAR:
                return "CHAR";
            case VARCHAR:
                return "VARCHAR";
            case LONGVARCHAR:
                return "LONGVARCHAR";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BINARY:
                return "BINARY";
            case VARBINARY:
                return "VARBINARY";
            case LONGVARBINARY:
                return "LONGVARBINARY";
            case NULL:
                return "NULL";
            case OTHER:
                return "OTHER";
            case JAVA_OBJECT:
                return "JAVA_OBJECT";
            case DISTINCT:
                return "DISTINCT";
            case STRUCT:
                return "STRUCT";
            case ARRAY:
                return "ARRAY";
            case BLOB:
                return "BLOB";
            case CLOB:
                return "CLOB";
            case REF:
                return "REF";
            case DATALINK:
                return "DATALINK";
            case BOOLEAN:
                return "BOOLEAN";
            case ROWID:
                return "ROWID";
            case NCHAR:
                return "NCHAR";
            case NVARCHAR:
                return "NVARCHAR";
            case LONGNVARCHAR:
                return "LONGNVARCHAR";
            case NCLOB:
                return "NCLOB";
            case SQLXML:
                return "SQLXML";
            default:
                return "Unknown";
        }
    }

    /**
     * Revert changes to java names made by user.
     */
    public void resetJavaName() {
        javaName.setValue(javaNamePrev);
    }

    /**
     * @return {@code true} if this field belongs to primary key.
     */
    public boolean key() {
        return key.get();
    }

    /**
     * @param pk {@code true} if this field belongs to primary key.
     */
    public void key(boolean pk) {
        key.set(pk);
    }

    /**
     * @return POJO field java name.
     */
    public String javaName() {
        return javaName.get();
    }

    /**
     * @param name New POJO field java name.
     */
    public void javaName(String name) {
        javaName.set(name);
    }

    /**
     * @return POJO field java type name.
     */
    public String javaTypeName() {
        return javaTypeName.get();
    }

    /**
     * @return Field name in database.
     */
    public String dbName() {
        return dbName.get();
    }

    /**
     * @return POJO field JDBC type in database.
     */
    public int dbType() {
        return dbType;
    }

    /**
     * @return Is NULL allowed for field in database.
     */
    public boolean nullable() {
        return nullable;
    }

    /**
     * @return List of possible java type conversions.
     */
    public ObservableList<String> conversions() {
        return conversions;
    }

    /**
     * @return Field type descriptor.
     */
    public CacheQueryTypeDescriptor descriptor() {
        desc.setJavaName(javaName());
        desc.setJavaType(classesMap.get(javaTypeName()));

        return desc;
    }

    /**
     * @return {@code true}
     */
    public boolean primitive() {
        return PRIMITIVES.contains(javaTypeName());
    }

    /**
     * @return {@code true} if field was changed by user.
     */
    public boolean changed() {
        return keyPrev != key() || !javaNamePrev.equals(javaName()) || !javaTypeNamePrev.equals(javaTypeName());
    }

    /**
     * @return Boolean property support for {@code key} property.
     */
    public BooleanProperty keyProperty() {
        return key;
    }

    /**
     * @return String property support for {@code javaName} property.
     */
    public StringProperty javaNameProperty() {
        return javaName;
    }

    /**
     * @return String property support for {@code javaTypeName} property.
     */
    public StringProperty javaTypeNameProperty() {
        return javaTypeName;
    }

    /**
     * @return String property support for {@code dbName} property.
     */
    public StringProperty dbNameProperty() {
        return dbName;
    }

    /**
     * @return String property support for {@code dbName} property.
     */
    public StringProperty dbTypeNameProperty() {
        return dbTypeName;
    }
}
