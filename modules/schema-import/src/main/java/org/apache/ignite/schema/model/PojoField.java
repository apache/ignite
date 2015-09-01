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
import java.util.List;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DISTINCT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.REF;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/**
 * Field descriptor with properties for JavaFX GUI bindings.
 */
public class PojoField {
    /** If this field should be used for code generation. */
    private final BooleanProperty useProp;

    /** If this field belongs to primary key. */
    private final BooleanProperty keyProp;

    /** If this field is an affinity key. */
    private final BooleanProperty akProp;

    /** If this field initially belongs to primary key. */
    private final boolean keyPrev;

    /** Field name in database. */
    private final StringProperty dbNameProp;

    /** Field type in database. */
    private final StringProperty dbTypeNameProp;

    /** Field name in POJO. */
    private final StringProperty javaNameProp;

    /** Initial field name in POJO. */
    private final String javaNamePrev;

    /** Field type in POJO. */
    private final StringProperty javaTypeNameProp;

    /** Initial field type in POJO. */
    private final String javaTypeNamePrev;

    /** Is {@code NULL} allowed for field in database. */
    private final boolean nullable;

    /** List of possible java type conversions. */
    private final ObservableList<String> conversions;

    /** Field owner. */
    private PojoDescriptor owner;

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

    /** Object types. */
    private static final List<String> OBJECTS = classNames(Boolean.class, Byte.class, Short.class, Integer.class,
        Long.class, Float.class, Double.class, BigDecimal.class);

    static {
        NOT_NULL_NUM_CONVERSIONS.addAll(PRIMITIVES);
        NOT_NULL_NUM_CONVERSIONS.addAll(OBJECTS);

        NULL_NUM_CONVERSIONS.addAll(OBJECTS);
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
     * @param dbName Field name in database.
     * @param dbType Field JDBC type in database.
     * @param javaName Field name in POJO.
     * @param javaTypeName Field type in POJO.
     * @param key {@code true} if this field belongs to primary key.
     * @param nullable {@code true} if  {@code NULL} allowed for field in database.
     */
    public PojoField(String dbName, int dbType, String javaName, String javaTypeName, boolean key, boolean nullable) {
        dbNameProp = new SimpleStringProperty(dbName);

        dbTypeNameProp = new SimpleStringProperty(jdbcTypeName(dbType));

        javaNamePrev = javaName;

        javaNameProp = new SimpleStringProperty(javaNamePrev);

        javaTypeNamePrev = javaTypeName;

        javaTypeNameProp = new SimpleStringProperty(javaTypeNamePrev);

        useProp = new SimpleBooleanProperty(true);

        keyPrev = key;

        keyProp = new SimpleBooleanProperty(keyPrev);

        this.nullable = nullable;

        akProp = new SimpleBooleanProperty(false);

        conversions = conversions(dbType, nullable, javaNamePrev);

        keyProp.addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> val, Boolean oldVal, Boolean newVal) {
                if (newVal) {
                    if (!use())
                        useProp.set(true);
                }
                else
                    akProp.set(false);
            }
        });

        akProp.addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> val, Boolean oldVal, Boolean newVal) {
                if (newVal && owner != null) {
                    keyProperty().set(true);

                    for (PojoField field : owner.fields())
                        if (field != PojoField.this && field.affinityKey())
                            field.akProp.set(false);
                }
            }
        });
    }

    /**
     * @param jdbcType String name for JDBC type.
     * @return String name for JDBC type.
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
        javaNameProp.set(javaNamePrev);
    }

    /**
     * @param owner New field owner.
     */
    public void owner(PojoDescriptor owner) {
        this.owner = owner;
    }

    /**
     * @return {@code true} if filed should be used for code generation.
     */
    public boolean use() {
        return useProp.get();
    }

    /**
     * @return {@code true} if this field belongs to primary key.
     */
    public boolean key() {
        return keyProp.get();
    }

    /**
     * @param pk {@code true} if this field belongs to primary key.
     */
    public void key(boolean pk) {
        keyProp.set(pk);
    }

    /**
     * @return {@code true} if this field is an affinity key.
     */
    public boolean affinityKey() {
        return akProp.get();
    }

    /**
     * @return POJO field java name.
     */
    public String javaName() {
        return javaNameProp.get();
    }

    /**
     * @param name New POJO field java name.
     */
    public void javaName(String name) {
        javaNameProp.set(name);
    }

    /**
     * @return POJO field java type name.
     */
    public String javaTypeName() {
        return javaTypeNameProp.get();
    }

    /**
     * @return Field name in database.
     */
    public String dbName() {
        return dbNameProp.get();
    }

    /**
     * @return POJO field JDBC type name in database.
     */
    public String dbTypeName() {
        return dbTypeNameProp.get();
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
     * @return {@code true} if type of field is primitive type.
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
     * @return Boolean property support for {@code use} property.
     */
    public BooleanProperty useProperty() {
        return useProp;
    }

    /**
     * @return Boolean property support for {@code key} property.
     */
    public BooleanProperty keyProperty() {
        return keyProp;
    }

    /**
     * @return Boolean property support for {@code affinityKey} property.
     */
    public BooleanProperty affinityKeyProperty() {
        return akProp;
    }

    /**
     * @return String property support for {@code javaName} property.
     */
    public StringProperty javaNameProperty() {
        return javaNameProp;
    }

    /**
     * @return String property support for {@code javaTypeName} property.
     */
    public StringProperty javaTypeNameProperty() {
        return javaTypeNameProp;
    }

    /**
     * @return String property support for {@code dbName} property.
     */
    public StringProperty dbNameProperty() {
        return dbNameProp;
    }

    /**
     * @return String property support for {@code dbName} property.
     */
    public StringProperty dbTypeNameProperty() {
        return dbTypeNameProp;
    }
}