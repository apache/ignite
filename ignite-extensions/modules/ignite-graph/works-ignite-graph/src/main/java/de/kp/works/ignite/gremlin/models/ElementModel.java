package de.kp.works.ignite.gremlin.models;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.query.IgniteResult;
import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import de.kp.works.ignite.gremlin.mutators.PropertyIncrementer;
import de.kp.works.ignite.gremlin.mutators.PropertyRemover;
import de.kp.works.ignite.gremlin.mutators.PropertyWriter;
import de.kp.works.ignite.gremlin.readers.LoadingElementReader;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("rawtypes")
public abstract class ElementModel extends BaseModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElementModel.class);

    public ElementModel(IgniteGraph graph, IgniteTable table) {
        super(graph, table);
    }

    public abstract LoadingElementReader getReader();

    /**
     * Load the element from the backing table.
     *
     * @param element The element
     */
    @SuppressWarnings("unchecked")
    public void load(Element element) {
        LOGGER.trace("Executing Get, type: {}, id: {}", getClass().getSimpleName(), element.id());

        IgniteResult result = table.get(element.id());
        if(result!=null) {
        	getReader().load(element, result);
        }
        else {
        	throw new IgniteGraphNotFoundException(element, "element does not exist: " + element.id());
        }
    }

    /**
     * Load the elements from the backing table.
     *
     * @param elements The elements
     */
    @SuppressWarnings("unchecked")
    public void load(List<? extends Element> elements) {
        LOGGER.trace("Executing Batch Get, type: {}", getClass().getSimpleName());

        List<Object> ids = elements.stream()
            .map(Element::id)
            .collect(Collectors.toList());

        IgniteResult[] results = table.get(ids);
        for (int i = 0; i < results.length; i++) {
            try {
                getReader().load(elements.get(i), results[i]);
            } catch (IgniteGraphNotFoundException e) {
                // ignore, the element will not have its properties fully loaded
            }
        }
    }

    /**
     * Delete the property entry from property table.
     *
     * @param element The element
     * @param key     The property key
     */
    public PropertyRemover clearProperty(Element element, ElementType elementType, String key) {
        return new PropertyRemover(graph, element, elementType, key);
    }

    /**
     * Write the given property to the property table.
     *
     * @param element The element
     * @param key     The property key
     * @param value   The property value
     */
    public PropertyWriter writeProperty(Element element, ElementType elementType, String key, Object value) {
        return new PropertyWriter(graph, element, elementType, key, value);
    }

    /**
     * Increment the given property in the property table.
     *
     * @param element The element
     * @param key     The property key
     * @param value   The amount to increment
     */
    public PropertyIncrementer incrementProperty(Element element, ElementType elementType, String key, long value) {
        return new PropertyIncrementer(graph, element, elementType, key, value);
    }

}
