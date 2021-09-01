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

package org.apache.ignite.internal.configuration.tree;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * {@link ConfigurationVisitor} implementation that converts a configuration tree to a combination of {@link Map} and
 * {@link List} objects.
 */
public class ConverterToMapVisitor implements ConfigurationVisitor<Object> {
    /** Include internal configuration nodes (private configuration extensions). */
    private final boolean includeInternal;

    /** Stack with intermediate results. Used to store values during recursive calls. */
    private final Deque<Object> deque = new ArrayDeque<>();

    /**
     * Constructor.
     *
     * @param includeInternal Include internal configuration nodes (private configuration extensions).
     */
    public ConverterToMapVisitor(boolean includeInternal) {
        this.includeInternal = includeInternal;
    }

    /** {@inheritDoc} */
    @Override public Object visitLeafNode(String key, Serializable val) {
        Object valObj = val;

        if (val instanceof Character)
            valObj = val.toString();
        else if (val != null && val.getClass().isArray())
            valObj = toListOfObjects(val);

        addToParent(key, valObj);

        return valObj;
    }

    /** {@inheritDoc} */
    @Override public Object visitInnerNode(String key, InnerNode node) {
        Map<String, Object> innerMap = new HashMap<>();

        deque.push(innerMap);

        node.traverseChildren(this, includeInternal);

        deque.pop();

        addToParent(key, innerMap);

        return innerMap;
    }

    /** {@inheritDoc} */
    @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> node) {
        List<Object> list = new ArrayList<>(node.size());

        deque.push(list);

        for (String subkey : node.namedListKeys()) {
            node.get(subkey).accept(subkey, this);

            ((Map<String, Object>)list.get(list.size() - 1)).put(node.syntheticKeyName(), subkey);
        }

        deque.pop();

        addToParent(key, list);

        return list;
    }

    /**
     * Adds a sub-element to the parent object if it exists.
     *
     * @param key Key for the passed element
     * @param val Value to add to the parent
     */
    private void addToParent(String key, Object val) {
        Object parent = deque.peek();

        if (parent instanceof Map)
            ((Map<String, Object>)parent).put(key, val);
        else if (parent instanceof List)
            ((Collection<Object>)parent).add(val);
    }

    /**
     * Converts array into a list of objects. Boxes array elements if they are primitive values.
     *
     * @param val Array of primitives or array of {@link String}s
     * @return List of objects corresponding to the passed array.
     */
    private List<?> toListOfObjects(Serializable val) {
        Stream<?> stream = IntStream.range(0, Array.getLength(val)).mapToObj(i -> Array.get(val, i));

        if (val.getClass().getComponentType() == char.class)
            stream = stream.map(Object::toString);

        return stream.collect(Collectors.toList());
    }
}
