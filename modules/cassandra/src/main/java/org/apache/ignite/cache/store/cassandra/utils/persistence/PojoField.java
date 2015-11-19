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
    /** TODO IGNITE-1371: add comment */
    private static final String NAME_ATTR = "name";

    /** TODO IGNITE-1371: add comment */
    private static final String COLUMN_ATTR = "column";

    /** TODO IGNITE-1371: add comment */
    private String name;

    /** TODO IGNITE-1371: add comment */
    private String col;
    /** TODO IGNITE-1371: add comment */
    private String colDDL;

    /** TODO IGNITE-1371: add comment */
    private PropertyDescriptor desc;

    /** TODO IGNITE-1371: add comment */
    public PojoField(Element el, Class pojoCls) {
        if (el == null)
            throw new IllegalArgumentException("DOM element representing POJO field object can't be null");

        if (!el.hasAttribute(NAME_ATTR)) {
            throw new IllegalArgumentException("DOM element representing POJO field object should have '"
                + NAME_ATTR + "' attribute");
        }

        this.name = el.getAttribute(NAME_ATTR).trim();
        this.col = el.hasAttribute(COLUMN_ATTR) ? el.getAttribute(COLUMN_ATTR).trim() : name.toLowerCase();

        init(PropertyMappingHelper.getPojoPropertyDescriptor(pojoCls, name));
    }

    /** TODO IGNITE-1371: add comment */
    public PojoField(PropertyDescriptor desc) {
        this.name = desc.getName();

        QuerySqlField sqlField = desc.getReadMethod() != null ?
            desc.getReadMethod().getAnnotation(QuerySqlField.class) :
            desc.getWriteMethod() == null ?
                null :
                desc.getWriteMethod().getAnnotation(QuerySqlField.class);

        this.col = sqlField != null && sqlField.name() != null ? sqlField.name() : name.toLowerCase();

        init(desc);

        if (sqlField != null)
            init(sqlField);
    }

    /** TODO IGNITE-1371: add comment */
    public String getName() {
        return name;
    }

    /** TODO IGNITE-1371: add comment */
    public String getColumn() {
        return col;
    }

    /** TODO IGNITE-1371: add comment */
    public String getColumnDDL() {
        return colDDL;
    }

    /** TODO IGNITE-1371: add comment */
    public Object getValueFromObject(Object obj, Serializer serializer) {
        try {
            Object val = desc.getReadMethod().invoke(obj);

            if (val == null)
                return null;

            DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(val.getClass());

            if (cassandraType != null)
                return val;

            if (serializer == null) {
                throw new IllegalStateException("Can't serialize value from object '" +
                    val.getClass().getName() + "' field '" + name + "', cause there is no BLOB serializer specified");
            }

            return serializer.serialize(val);
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to get value of the field '" + desc.getName() + "' from the instance " +
                " of '" + obj.getClass().toString() + "' class", e);
        }
    }

    /** TODO IGNITE-1371: add comment */
    public void setValueFromRow(Row row, Object obj, Serializer serializer) {
        Object val = PropertyMappingHelper.getCassandraColumnValue(row, col, desc.getPropertyType(), serializer);

        try {
            desc.getWriteMethod().invoke(obj, val);
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to set value of the field '" + desc.getName() + "' of the instance " +
                " of '" + obj.getClass().toString() + "' class", e);
        }
    }

    /** TODO IGNITE-1371: add comment */
    protected abstract void init(QuerySqlField sqlField);

    /** TODO IGNITE-1371: add comment */
    protected void init(PropertyDescriptor desc) {
        if (desc.getReadMethod() == null) {
            throw new IllegalArgumentException("Field '" + desc.getName() +
                "' of the class instance '" + desc.getPropertyType().getName() +
                "' doesn't provide getter method");
        }

        if (desc.getWriteMethod() == null) {
            throw new IllegalArgumentException("Field '" + desc.getName() +
                "' of POJO object instance of the class '" + desc.getPropertyType().getName() +
                "' doesn't provide write method");
        }

        if (!desc.getReadMethod().isAccessible())
            desc.getReadMethod().setAccessible(true);

        if (!desc.getWriteMethod().isAccessible())
            desc.getWriteMethod().setAccessible(true);

        DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(desc.getPropertyType());
        cassandraType = cassandraType == null ? DataType.Name.BLOB : cassandraType;

        this.desc = desc;
        this.colDDL = col + " " + cassandraType.toString();
    }
}
