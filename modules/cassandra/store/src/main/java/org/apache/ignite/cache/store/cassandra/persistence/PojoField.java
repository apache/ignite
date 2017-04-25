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
import com.datastax.driver.core.Row;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.w3c.dom.Element;

/**
 * Descriptor for particular field in a POJO object, specifying how this field
 * should be written to or loaded from Cassandra.
 */
public abstract class PojoField implements Serializable {
    /** Name attribute of XML element describing Pojo field. */
    private static final String NAME_ATTR = "name";

    /** Column attribute of XML element describing Pojo field. */
    private static final String COLUMN_ATTR = "column";

    /** Field name. */
    private String name;

    /** Java class to which the field belongs. */
    private Class objJavaCls;

    /** Field column name in Cassandra table. */
    private String col;

    /** Field column DDL.  */
    private String colDDL;

    /** Indicator for calculated field. */
    private Boolean calculated;

    /** Field property descriptor. */
    private transient PropertyDescriptor desc;

    /**
     * Creates instance of {@link PojoField} based on it's description in XML element.
     *
     * @param el XML element describing Pojo field
     * @param pojoCls Pojo java class.
     */
    public PojoField(Element el, Class<?> pojoCls) {
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

    /**
     * Creates instance of {@link PojoField}  from its property descriptor.
     *
     * @param desc Field property descriptor.
     */
    public PojoField(PropertyDescriptor desc) {
        this.name = desc.getName();

        col = name.toLowerCase();

        init(desc);
    }

    /**
     * @return field name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns java class of the field.
     *
     * @return Java class.
     */
    public Class getJavaClass() {
        return propDesc().getPropertyType();
    }

    /**
     * @return Cassandra table column name.
     */
    public String getColumn() {
        return col;
    }

    /**
     * @return Cassandra table column DDL statement.
     */
    public String getColumnDDL() {
        return colDDL;
    }

    /**
     * Indicates if it's a calculated field - field which value just generated based on other field values.
     * Such field will be stored in Cassandra as all other POJO fields, but it's value shouldn't be read from
     * Cassandra - cause it's again just generated based on other field values. One of the good applications of such
     * kind of fields - Cassandra materialized views build on top of other tables.
     *
     * @return {@code true} if it's auto generated field, {@code false} if not.
     */
    public boolean calculatedField() {
        if (calculated != null)
            return calculated;

        return calculated = propDesc().getWriteMethod() == null;
    }

    /**
     * Gets field value as an object having Cassandra compatible type.
     * This it could be stored directly into Cassandra without any conversions.
     *
     * @param obj Object instance.
     * @param serializer {@link org.apache.ignite.cache.store.cassandra.serializer.Serializer} to use.
     * @return Object to store in Cassandra table column.
     */
    public Object getValueFromObject(Object obj, Serializer serializer) {
        try {
            Object val = propDesc().getReadMethod().invoke(obj);

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
            throw new IgniteException("Failed to get value of the field '" + name + "' from the instance " +
                " of '" + obj.getClass().toString() + "' class", e);
        }
    }

    /**
     * Sets object field value from a {@link com.datastax.driver.core.Row} returned by Cassandra CQL statement.
     *
     * @param row {@link com.datastax.driver.core.Row}
     * @param obj object which field should be populated from {@link com.datastax.driver.core.Row}
     * @param serializer {@link org.apache.ignite.cache.store.cassandra.serializer.Serializer} to use.
     */
    public void setValueFromRow(Row row, Object obj, Serializer serializer) {
        if (calculatedField())
            return;

        Object val = PropertyMappingHelper.getCassandraColumnValue(row, col, propDesc().getPropertyType(), serializer);

        try {
            propDesc().getWriteMethod().invoke(obj, val);
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to set value of the field '" + name + "' of the instance " +
                " of '" + obj.getClass().toString() + "' class", e);
        }
    }

    /**
     * Initializes field info from property descriptor.
     *
     * @param desc {@link PropertyDescriptor} descriptor.
     */
    protected void init(PropertyDescriptor desc) {
        if (desc.getReadMethod() == null) {
            throw new IllegalArgumentException("Field '" + desc.getName() +
                "' of the class instance '" + desc.getPropertyType().getName() +
                "' doesn't provide getter method");
        }

        if (!desc.getReadMethod().isAccessible())
            desc.getReadMethod().setAccessible(true);

        if (desc.getWriteMethod() != null && !desc.getWriteMethod().isAccessible())
            desc.getWriteMethod().setAccessible(true);

        DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(desc.getPropertyType());
        cassandraType = cassandraType == null ? DataType.Name.BLOB : cassandraType;

        this.objJavaCls = desc.getReadMethod().getDeclaringClass();
        this.desc = desc;
        this.colDDL = "\"" + col + "\" " + cassandraType.toString();
    }

    /**
     * Returns property descriptor of the POJO field
     *
     * @return Property descriptor
     */
    private PropertyDescriptor propDesc() {
        return desc != null ? desc : (desc = PropertyMappingHelper.getPojoPropertyDescriptor(objJavaCls, name));
    }
}
