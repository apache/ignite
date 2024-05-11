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
import de.kp.works.ignite.mutate.IgniteDelete;
import de.kp.works.ignite.mutate.IgniteMutation;
import de.kp.works.ignite.gremlin.IgniteGraph;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyRemover implements Mutator {

    private final Element element;
    private final ElementType elementType;

    private final String key;

    public PropertyRemover(IgniteGraph graph, Element element, ElementType elementType, String key) {
        this.element = element;
        this.elementType = elementType;

        this.key = key;
    }

    @Override
    public Iterator<IgniteMutation> constructMutations() {

        Object id = element.id();

        IgniteDelete delete = new IgniteDelete(id, elementType);
        delete.addColumn(key);

        return IteratorUtils.of(delete);
    }
}
