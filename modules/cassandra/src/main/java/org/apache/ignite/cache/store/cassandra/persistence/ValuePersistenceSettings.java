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

import java.beans.PropertyDescriptor;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Stores persistence settings for Ignite cache value
 */
public class ValuePersistenceSettings extends PersistenceSettings {
    /** XML element describing value field settings. */
    private static final String FIELD_ELEMENT = "field";

    /** Value fields. */
    private List<PojoField> fields = new LinkedList<>();

    /**
     * Creates class instance from XML configuration.
     *
     * @param el XML element describing value persistence settings.
     */
    public ValuePersistenceSettings(Element el) {
        super(el);

        if (!PersistenceStrategy.POJO.equals(getStrategy()))
            return;

        NodeList nodes = el.getElementsByTagName(FIELD_ELEMENT);

        fields = detectFields(nodes);

        if (fields.isEmpty())
            throw new IllegalStateException("Failed to initialize value fields for class '" + getJavaClass().getName() + "'");

        checkDuplicates(fields);
    }

    /**
     * @return List of value fields.
     */
    public List<PojoField> getFields() {
        return fields == null ? null : Collections.unmodifiableList(fields);
    }

    /** {@inheritDoc} */
    @Override protected String defaultColumnName() {
        return "value";
    }

    /**
     * Extracts POJO fields from a list of corresponding XML field nodes.
     *
     * @param fieldNodes Field nodes to process.
     * @return POJO fields list.
     */
    private List<PojoField> detectFields(NodeList fieldNodes) {
        List<PojoField> list = new LinkedList<>();

        if (fieldNodes == null || fieldNodes.getLength() == 0) {
            List<PropertyDescriptor> primitivePropDescriptors = PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(), true);
            for (PropertyDescriptor descriptor : primitivePropDescriptors)
                list.add(new PojoValueField(descriptor));

            return list;
        }

        List<PropertyDescriptor> allPropDescriptors = PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(), false);

        int cnt = fieldNodes.getLength();

        for (int i = 0; i < cnt; i++) {
            PojoValueField field = new PojoValueField((Element)fieldNodes.item(i), getJavaClass());

            PropertyDescriptor desc = findPropertyDescriptor(allPropDescriptors, field.getName());

            if (desc == null) {
                throw new IllegalArgumentException("Specified POJO field '" + field.getName() +
                    "' doesn't exist in '" + getJavaClass().getName() + "' class");
            }

            list.add(field);
        }

        return list;
    }
}
