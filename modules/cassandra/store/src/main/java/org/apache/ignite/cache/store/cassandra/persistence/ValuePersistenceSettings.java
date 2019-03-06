/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.store.cassandra.persistence;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

        if (PersistenceStrategy.POJO != getStrategy()) {
            init();

            return;
        }

        NodeList nodes = el.getElementsByTagName(FIELD_ELEMENT);

        fields = detectPojoFields(nodes);

        if (fields.isEmpty())
            throw new IllegalStateException("Failed to initialize value fields for class '" + getJavaClass().getName() + "'");

        checkDuplicates(fields);

        init();
    }

    /**
     * @return List of value fields.
     */
    @Override public List<PojoField> getFields() {
        return fields == null ? null : Collections.unmodifiableList(fields);
    }

    /** {@inheritDoc} */
    @Override protected String defaultColumnName() {
        return "value";
    }

    /** {@inheritDoc} */
    @Override protected PojoField createPojoField(Element el, Class clazz) {
        return new PojoValueField(el, clazz);
    }

    /** {@inheritDoc} */
    @Override protected PojoField createPojoField(PojoFieldAccessor accessor) {
        return new PojoValueField(accessor);
    }
}
