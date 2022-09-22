package de.kp.works.ignite.gremlin.mutators;
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
import de.kp.works.ignite.mutate.IgniteIncrement;
import de.kp.works.ignite.mutate.IgniteMutation;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.ValueUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyIncrementer implements Mutator {

    private final Element element;
    private final ElementType elementType;

    private final String key;
    private final long value;

    public PropertyIncrementer(IgniteGraph graph, Element element, ElementType elementType, String key, long value) {
        this.element = element;
        this.elementType = elementType;

        this.key = key;
        this.value = value;
    }

    @Override
    public Iterator<IgniteMutation> constructMutations() {

        Object id = element.id();

        IgniteIncrement increment = new IgniteIncrement(id, elementType);

        String colType = ValueUtils.getValueType(value).name();
        increment.addColumn(key, colType, value);

        return IteratorUtils.of(increment);
    }
}
