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

package org.apache.ignite.cache.store.cassandra.utils.persistence;

import com.datastax.driver.core.DataType;
import java.beans.PropertyDescriptor;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.cache.store.cassandra.utils.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.utils.serializer.JavaSerializer;
import org.apache.ignite.cache.store.cassandra.utils.serializer.Serializer;
import org.w3c.dom.Element;

/**
 * Stores persistence settings, which describes how particular key/value from Ignite cache
 * should be stored in Cassandra
 */
public abstract class PersistenceSettings {
    private static final String STRATEGY_ATTR = "strategy";
    private static final String COLUMN_ATTR = "column";
    private static final String SERIALIZER_ATTR = "serializer";
    private static final String CLASS_ATTR = "class";

    private PersistenceStrategy strategy;
    private Class javaClass;
    private String column;
    private Serializer serializer = new JavaSerializer();

    public static PropertyDescriptor findPropertyDescriptor(List<PropertyDescriptor> descriptors, String propertyName) {
        if (descriptors == null || descriptors.isEmpty() || propertyName == null || propertyName.trim().isEmpty())
            return null;

        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getName().equals(propertyName))
                return descriptor;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public PersistenceSettings(Element el) {
        if (el == null)
            throw new IllegalArgumentException("DOM element representing key/value persistence object can't be null");

        if (!el.hasAttribute(STRATEGY_ATTR)) {
            throw new IllegalArgumentException("DOM element representing key/value persistence object should have '" +
                STRATEGY_ATTR + "' attribute");
        }

        try {
            strategy = PersistenceStrategy.valueOf(el.getAttribute(STRATEGY_ATTR).trim().toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Incorrect persistence strategy specified: " + el.getAttribute(STRATEGY_ATTR));
        }

        if (!el.hasAttribute(CLASS_ATTR) && !PersistenceStrategy.BLOB.equals(strategy)) {
            throw new IllegalArgumentException("DOM element representing key/value persistence object should have '" +
                CLASS_ATTR + "' attribute or have BLOB persistence strategy");
        }

        try {
            javaClass = el.hasAttribute(CLASS_ATTR) ? getClassInstance(el.getAttribute(CLASS_ATTR).trim()) : null;
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("Incorrect java class specified '" + el.getAttribute(CLASS_ATTR) + "' " +
                "for Cassandra persistence", e);
        }

        if (!PersistenceStrategy.BLOB.equals(strategy) &&
            (javaClass.equals(ByteBuffer.class) || javaClass.equals(byte[].class))) {
            throw new IllegalArgumentException("Java class '" + el.getAttribute(CLASS_ATTR) + "' " +
                "specified could only be persisted using BLOB persistence strategy");
        }

        if (PersistenceStrategy.PRIMITIVE.equals(strategy) &&
            PropertyMappingHelper.getCassandraType(javaClass) == null) {
            throw new IllegalArgumentException("Current implementation doesn't support persisting '" +
                javaClass.getName() + "' object using PRIMITIVE strategy");
        }

        if (PersistenceStrategy.POJO.equals(strategy)) {
            try {
                javaClass.getConstructor();
            }
            catch (Throwable e) {
                throw new IllegalArgumentException("Java class '" + javaClass.getName() + "' couldn't be used as POJO " +
                    "cause it doesn't have no arguments constructor", e);
            }
        }

        if (el.hasAttribute(COLUMN_ATTR)) {
            if (!PersistenceStrategy.BLOB.equals(strategy) && !PersistenceStrategy.PRIMITIVE.equals(strategy)) {
                throw new IllegalArgumentException("Incorrect configuration of Cassandra key/value persistence settings, " +
                    "'" + COLUMN_ATTR + "' attribute is only applicable for PRIMITIVE or BLOB strategy");
            }

            column = el.getAttribute(COLUMN_ATTR).trim();
        }

        if (el.hasAttribute(SERIALIZER_ATTR)) {
            if (!PersistenceStrategy.BLOB.equals(strategy) && !PersistenceStrategy.POJO.equals(strategy)) {
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

        if ((PersistenceStrategy.BLOB.equals(strategy) || PersistenceStrategy.PRIMITIVE.equals(strategy)) && column == null)
            column = defaultColumnName();
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public PersistenceStrategy getStrategy() {
        return strategy;
    }

    public String getColumn() {
        return column;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public abstract List<PojoField> getFields();

    public String getTableColumnsDDL() {
        if (PersistenceStrategy.BLOB.equals(strategy)) {
            return column + " " + DataType.Name.BLOB.toString();
        }

        if (PersistenceStrategy.PRIMITIVE.equals(strategy)) {
            return column + " " + PropertyMappingHelper.getCassandraType(javaClass);
        }

        StringBuilder builder = new StringBuilder();

        for (PojoField field : getFields()) {
            if (builder.length() > 0)
                builder.append(", ");

            builder.append(field.getColumnDDL());
        }

        if (builder.length() == 0) {
            throw new IllegalStateException("There are no POJO fields found for '" + javaClass.toString()
                + "' class to be presented as a Cassandra primary key");
        }

        return builder.toString();
    }

    protected abstract String defaultColumnName();

    protected void checkDuplicates(List<PojoField> fields) {
        if (fields == null || fields.isEmpty())
            return;

        for (PojoField field1 : fields) {
            boolean sameNames = false;
            boolean sameColumns = false;

            for (PojoField field2 : fields) {
                if (field1.getName().equals(field2.getName())) {
                    if (sameNames) {
                        throw new IllegalArgumentException("Incorrect Cassandra key persistence settings," +
                            " two POJO fields with the same name '" + field1.getName() + "' specified");
                    }

                    sameNames = true;
                }

                if (field1.getColumn().equals(field2.getColumn())) {
                    if (sameColumns) {
                        throw new IllegalArgumentException("Incorrect Cassandra persistence settings," +
                            " two POJO fields with the same column '" + field1.getColumn() + "' specified");
                    }

                    sameColumns = true;
                }
            }
        }
    }

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

        throw new RuntimeException("Failed to load class '" + clazz + "' using reflection");
    }

    private Object newObjectInstance(String clazz) {
        try {
            return getClassInstance(clazz).newInstance();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to instantiate class '" + clazz +
                "' using default constructor", e);
        }
    }

}
