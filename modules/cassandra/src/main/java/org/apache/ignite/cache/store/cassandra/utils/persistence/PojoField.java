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
import com.datastax.driver.core.Row;
import java.beans.PropertyDescriptor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.cassandra.utils.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.utils.serializer.Serializer;
import org.w3c.dom.Element;

/**
 * Descriptor for particular field in a POJO object, specifying how this field
 * should be written to or loaded from Cassandra
 */
public abstract class PojoField {
    private static final String NAME_ATTR = "name";
    private static final String COLUMN_ATTR = "column";

    private String name;
    private String column;
    private String columnDDL;
    private PropertyDescriptor descriptor;

    public PojoField(Element el, Class pojoClass) {
        if (el == null)
            throw new IllegalArgumentException("DOM element representing POJO field object can't be null");

        if (!el.hasAttribute(NAME_ATTR)) {
            throw new IllegalArgumentException("DOM element representing POJO field object should have '"
                + NAME_ATTR + "' attribute");
        }

        this.name = el.getAttribute(NAME_ATTR).trim();
        this.column = el.hasAttribute(COLUMN_ATTR) ? el.getAttribute(COLUMN_ATTR).trim() : name.toLowerCase();

        init(PropertyMappingHelper.getPojoPropertyDescriptor(pojoClass, name));
    }

    public PojoField(PropertyDescriptor descriptor) {
        this.name = descriptor.getName();

        QuerySqlField sqlField = descriptor.getReadMethod() != null ?
            descriptor.getReadMethod().getAnnotation(QuerySqlField.class) :
            descriptor.getWriteMethod() == null ?
                null :
                descriptor.getWriteMethod().getAnnotation(QuerySqlField.class);

        this.column = sqlField != null && sqlField.name() != null ? sqlField.name() : name.toLowerCase();

        init(descriptor);

        if (sqlField != null)
            init(sqlField);
    }

    public String getName() {
        return name;
    }

    public String getColumn() {
        return column;
    }

    public String getColumnDDL() {
        return columnDDL;
    }

    public Object getValueFromObject(Object obj, Serializer serializer) {
        try {
            Object value = descriptor.getReadMethod().invoke(obj);

            if (value == null)
                return null;

            DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(value.getClass());

            if (cassandraType != null)
                return value;

            if (serializer == null) {
                throw new IllegalStateException("Can't serialize value from object '" +
                    value.getClass().getName() + "' field '" + name + "', cause there is no BLOB serializer specified");
            }

            return serializer.serialize(value);
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to get value of the field '" + descriptor.getName() + "' from the instance " +
                " of '" + obj.getClass().toString() + "' class", e);
        }
    }

    public void setValueFromRow(Row row, Object obj, Serializer serializer) {
        Object value = PropertyMappingHelper.getCassandraColumnValue(row, column, descriptor.getPropertyType(), serializer);

        try {
            descriptor.getWriteMethod().invoke(obj, value);
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to set value of the field '" + descriptor.getName() + "' of the instance " +
                " of '" + obj.getClass().toString() + "' class", e);
        }
    }

    protected void init(QuerySqlField sqlField) {
    }

    protected void init(PropertyDescriptor descriptor) {
        if (descriptor.getReadMethod() == null) {
            throw new IllegalArgumentException("Field '" + descriptor.getName() +
                "' of the class instance '" + descriptor.getPropertyType().getName() +
                "' doesn't provide getter method");
        }

        if (descriptor.getWriteMethod() == null) {
            throw new IllegalArgumentException("Field '" + descriptor.getName() +
                "' of POJO object instance of the class '" + descriptor.getPropertyType().getName() +
                "' doesn't provide write method");
        }

        if (!descriptor.getReadMethod().isAccessible())
            descriptor.getReadMethod().setAccessible(true);

        if (!descriptor.getWriteMethod().isAccessible())
            descriptor.getWriteMethod().setAccessible(true);

        DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(descriptor.getPropertyType());
        cassandraType = cassandraType == null ? DataType.Name.BLOB : cassandraType;

        this.descriptor = descriptor;
        this.columnDDL = column + " " + cassandraType.toString();
    }
}
