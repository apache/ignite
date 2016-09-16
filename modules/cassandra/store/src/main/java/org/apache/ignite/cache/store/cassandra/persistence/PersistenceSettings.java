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

package org.apache.ignite.cache.store.cassandra.persistence;

import com.datastax.driver.core.DataType;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.w3c.dom.Element;

/**
 * Stores persistence settings, which describes how particular key/value
 * from Ignite cache should be stored in Cassandra.
 */
public abstract class PersistenceSettings implements Serializable {
    /** Xml attribute specifying persistence strategy. */
    private static final String STRATEGY_ATTR = "strategy";

    /** Xml attribute specifying Cassandra column name. */
    private static final String COLUMN_ATTR = "column";

    /** Xml attribute specifying BLOB serializer to use. */
    private static final String SERIALIZER_ATTR = "serializer";

    /** Xml attribute specifying java class of the object to be persisted. */
    private static final String CLASS_ATTR = "class";

    /** Persistence strategy to use. */
    private PersistenceStrategy stgy;

    /** Java class of the object to be persisted. */
    private Class javaCls;

    /** Cassandra table column name where object should be persisted in
     *  case of using BLOB or PRIMITIVE persistence strategy. */
    private String col;

    /** Serializer for BLOBs. */
    private Serializer serializer = new JavaSerializer();

    /**
     * Extracts property descriptor from the descriptors list by its name.
     *
     * @param descriptors descriptors list.
     * @param propName property name.
     *
     * @return property descriptor.
     */
    public static PropertyDescriptor findPropertyDescriptor(List<PropertyDescriptor> descriptors, String propName) {
        if (descriptors == null || descriptors.isEmpty() || propName == null || propName.trim().isEmpty())
            return null;

        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getName().equals(propName))
                return descriptor;
        }

        return null;
    }

    /**
     * Constructs persistence settings from corresponding XML element.
     *
     * @param el xml element containing persistence settings configuration.
     */
    @SuppressWarnings("unchecked")
    public PersistenceSettings(Element el) {
        if (el == null)
            throw new IllegalArgumentException("DOM element representing key/value persistence object can't be null");

        if (!el.hasAttribute(STRATEGY_ATTR)) {
            throw new IllegalArgumentException("DOM element representing key/value persistence object should have '" +
                STRATEGY_ATTR + "' attribute");
        }

        try {
            stgy = PersistenceStrategy.valueOf(el.getAttribute(STRATEGY_ATTR).trim().toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Incorrect persistence strategy specified: " + el.getAttribute(STRATEGY_ATTR));
        }

        if (!el.hasAttribute(CLASS_ATTR) && !PersistenceStrategy.BLOB.equals(stgy)) {
            throw new IllegalArgumentException("DOM element representing key/value persistence object should have '" +
                CLASS_ATTR + "' attribute or have BLOB persistence strategy");
        }

        try {
            javaCls = el.hasAttribute(CLASS_ATTR) ? getClassInstance(el.getAttribute(CLASS_ATTR).trim()) : null;
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("Incorrect java class specified '" + el.getAttribute(CLASS_ATTR) + "' " +
                "for Cassandra persistence", e);
        }

        if (!PersistenceStrategy.BLOB.equals(stgy) &&
            (ByteBuffer.class.equals(javaCls) || byte[].class.equals(javaCls))) {
            throw new IllegalArgumentException("Java class '" + el.getAttribute(CLASS_ATTR) + "' " +
                "specified could only be persisted using BLOB persistence strategy");
        }

        if (PersistenceStrategy.PRIMITIVE.equals(stgy) &&
            PropertyMappingHelper.getCassandraType(javaCls) == null) {
            throw new IllegalArgumentException("Current implementation doesn't support persisting '" +
                javaCls.getName() + "' object using PRIMITIVE strategy");
        }

        if (PersistenceStrategy.POJO.equals(stgy)) {
            if (javaCls == null)
                throw new IllegalStateException("Object java class should be specified for POJO persistence strategy");

            try {
                javaCls.getConstructor();
            }
            catch (Throwable e) {
                throw new IllegalArgumentException("Java class '" + javaCls.getName() + "' couldn't be used as POJO " +
                    "cause it doesn't have no arguments constructor", e);
            }
        }

        if (el.hasAttribute(COLUMN_ATTR)) {
            if (!PersistenceStrategy.BLOB.equals(stgy) && !PersistenceStrategy.PRIMITIVE.equals(stgy)) {
                throw new IllegalArgumentException("Incorrect configuration of Cassandra key/value persistence settings, " +
                    "'" + COLUMN_ATTR + "' attribute is only applicable for PRIMITIVE or BLOB strategy");
            }

            col = el.getAttribute(COLUMN_ATTR).trim();
        }

        if (el.hasAttribute(SERIALIZER_ATTR)) {
            if (!PersistenceStrategy.BLOB.equals(stgy) && !PersistenceStrategy.POJO.equals(stgy)) {
                throw new IllegalArgumentException("Incorrect configuration of Cassandra key/value persistence settings, " +
                    "'" + SERIALIZER_ATTR + "' attribute is only applicable for BLOB and POJO strategies");
            }

            Object obj = newObjectInstance(el.getAttribute(SERIALIZER_ATTR).trim());

            if (!(obj instanceof Serializer)) {
                throw new IllegalArgumentException("Incorrect configuration of Cassandra key/value persistence settings, " +
                    "serializer class '" + el.getAttribute(SERIALIZER_ATTR) + "' doesn't implement '" +
                    Serializer.class.getName() + "' interface");
            }

            serializer = (Serializer)obj;
        }

        if ((PersistenceStrategy.BLOB.equals(stgy) || PersistenceStrategy.PRIMITIVE.equals(stgy)) && col == null)
            col = defaultColumnName();
    }

    /**
     * Returns java class of the object to be persisted.
     *
     * @return java class.
     */
    public Class getJavaClass() {
        return javaCls;
    }

    /**
     * Returns persistence strategy to use.
     *
     * @return persistence strategy.
     */
    public PersistenceStrategy getStrategy() {
        return stgy;
    }

    /**
     * Returns Cassandra table column name where object should be persisted in
     * case of using BLOB or PRIMITIVE persistence strategy.
     *
     * @return column name.
     */
    public String getColumn() {
        return col;
    }

    /**
     * Returns serializer to be used for BLOBs.
     *
     * @return serializer.
     */
    public Serializer getSerializer() {
        return serializer;
    }

    /**
     * Returns list of POJO fields to be persisted.
     *
     * @return list of fields.
     */
    public abstract List<PojoField> getFields();

    /**
     * Returns Cassandra table columns DDL, corresponding to POJO fields which should be persisted.
     *
     * @return DDL statement for Cassandra table fields
     */
    public String getTableColumnsDDL() {
        if (PersistenceStrategy.BLOB.equals(stgy))
            return "  \"" + col + "\" " + DataType.Name.BLOB.toString();

        if (PersistenceStrategy.PRIMITIVE.equals(stgy))
            return "  \"" + col + "\" " + PropertyMappingHelper.getCassandraType(javaCls);

        StringBuilder builder = new StringBuilder();

        for (PojoField field : getFields()) {
            if (builder.length() > 0)
                builder.append(",\n");

            builder.append("  ").append(field.getColumnDDL());
        }

        if (builder.length() == 0) {
            throw new IllegalStateException("There are no POJO fields found for '" + javaCls.toString()
                + "' class to be presented as a Cassandra primary key");
        }

        return builder.toString();
    }

    /**
     * Returns default name for Cassandra column (if it's not specified explicitly).
     *
     * @return column name
     */
    protected abstract String defaultColumnName();

    /**
     * Checks if there are POJO filed with the same name or same Cassandra column specified in persistence settings
     *
     * @param fields list of fields to be persisted into Cassandra
     */
    protected void checkDuplicates(List<PojoField> fields) {
        if (fields == null || fields.isEmpty())
            return;

        for (PojoField field1 : fields) {
            boolean sameNames = false;
            boolean sameCols = false;

            for (PojoField field2 : fields) {
                if (field1.getName().equals(field2.getName())) {
                    if (sameNames) {
                        throw new IllegalArgumentException("Incorrect Cassandra key persistence settings, " +
                            "two POJO fields with the same name '" + field1.getName() + "' specified");
                    }

                    sameNames = true;
                }

                if (field1.getColumn().equals(field2.getColumn())) {
                    if (sameCols) {
                        throw new IllegalArgumentException("Incorrect Cassandra persistence settings, " +
                            "two POJO fields with the same column '" + field1.getColumn() + "' specified");
                    }

                    sameCols = true;
                }
            }
        }
    }

    /**
     * Instantiates Class object for particular class
     *
     * @param clazz class name
     * @return Class object
     */
    private Class getClassInstance(String clazz) {
        try {
            return Class.forName(clazz);
        }
        catch (ClassNotFoundException ignored) {
        }

        try {
            return Class.forName(clazz, true, Thread.currentThread().getContextClassLoader());
        }
        catch (ClassNotFoundException ignored) {
        }

        try {
            return Class.forName(clazz, true, PersistenceSettings.class.getClassLoader());
        }
        catch (ClassNotFoundException ignored) {
        }

        try {
            return Class.forName(clazz, true, ClassLoader.getSystemClassLoader());
        }
        catch (ClassNotFoundException ignored) {
        }

        throw new IgniteException("Failed to load class '" + clazz + "' using reflection");
    }

    /**
     * Creates new object instance of particular class
     *
     * @param clazz class name
     * @return object
     */
    private Object newObjectInstance(String clazz) {
        try {
            return getClassInstance(clazz).newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate class '" + clazz + "' using default constructor", e);
        }
    }

}
