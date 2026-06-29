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

package org.apache.ignite.internal.util.tostring;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.ignite.internal.util.tostring.GridToStringNode.INNER_BUILDER;
import static org.apache.ignite.internal.util.tostring.GridToStringNode.identities;

/**
 * A factory class responsible for creating appropriate GridToStringNode instances
 * based on the type and value of the object being processed.
 */
public class GridToStringNodeFactory {
    /**
     * Creates a list of nodes from arrays of property names and values.
     * Handles sensitive data exclusion and node reuse from a thread-local cache.
     * @param addNames Array of property names.
     * @param addVals Array of property values.
     * @param addSens Array of flags indicating if a property is sensitive.
     * @param addLen The number of elements to process.
     * @return A list of constructed GridToStringNode objects.
     */
    static List<GridToStringNode> getNodes(Object[] addNames,
                                           Object[] addVals,
                                           boolean[] addSens,
                                           int addLen) {
        List<GridToStringNode> result = new LinkedList<>();
        boolean includeSensitive = GridToStringBuilder.includeSensitive();
        for (int i = 0; i < addLen; i++) {
            if (!includeSensitive && shouldBeExcluded(addVals, addSens, i))
                continue;
            String propName = String.valueOf(addNames[i]);
            int idx = i;
            GridToStringNode node = getGridToStringNode(propName, () -> addVals[idx], () -> addVals[idx].getClass());
            result.add(node);
        }
        return result;
    }

    /**
     * Creates a node for a field based on its descriptor and the parent object.
     * This method acts as a dispatcher, routing the creation logic based on the field's type.
     * @param obj The parent object containing the field.
     * @param fd The descriptor of the field to be processed.
     * @return A new GridToStringNode for the field's value.
     */
    static GridToStringNode getGridToStringNode(Object obj, GridToStringFieldDescriptor fd) {
        String childPropName = fd.getName();
        if (obj == null)
            return new GridToStringNullNode(childPropName);
        return switch (fd.type()) {
            case GridToStringFieldDescriptor.FIELD_TYPE_OBJECT -> {
                Supplier<Class<?>> fieldClsSupplier = () -> Optional.of(fd)
                        .map(GridToStringFieldDescriptor::fieldClass)
                        .map(Class.class::cast)
                        .orElseGet(obj::getClass);
                yield getGridToStringNode(childPropName, () -> fd.objectValue(obj), fieldClsSupplier);
            }
            case GridToStringFieldDescriptor.FIELD_TYPE_BYTE ->
                    getGridToStringNode(childPropName, () -> fd.byteValue(obj), () -> byte.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_BOOLEAN ->
                    getGridToStringNode(childPropName, () -> fd.booleanValue(obj), () -> boolean.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_CHAR ->
                    getGridToStringNode(childPropName, () -> fd.charValue(obj), () -> char.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_SHORT ->
                    getGridToStringNode(childPropName, () -> fd.shortValue(obj), () -> short.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_INT ->
                    getGridToStringNode(childPropName, () -> fd.intField(obj), () -> int.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_FLOAT ->
                    getGridToStringNode(childPropName, () -> fd.floatField(obj), () -> float.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_LONG ->
                    getGridToStringNode(childPropName, () -> fd.longField(obj), () -> long.class);
            case GridToStringFieldDescriptor.FIELD_TYPE_DOUBLE ->
                    getGridToStringNode(childPropName, () -> fd.doubleField(obj), () -> double.class);
            default -> throw new IllegalStateException("Unexpected field descriptor type: " + fd.type());
        };
    }

    /**
     * The core factory method that creates a node for a given value and its class.
     * It is the central point for determining the correct node type for any object.
     * Handles nulls, recursion, primitives, arrays, collections, maps, and standard objects.
     * @param childPropName The property name for the new node.
     * @param valSupplier A supplier to lazily retrieve the value.
     * @param childFieldClsSupplier A supplier to lazily retrieve the class of the value.
     * @return A new GridToStringNode appropriate for the value.
     */
    static GridToStringNode getGridToStringNode(String childPropName,
                                                Supplier<Object> valSupplier,
                                                Supplier<Class<?>> childFieldClsSupplier) {
        Object val = valSupplier.get();
        if (val == null)
            return new GridToStringNullNode(childPropName);
        Optional<GridToStringNode> recursionTermination = NodeRecursionMonitor.findRecursionMonitor(val)
                .map(monitor -> GridToStringRecursionTerminationNode.of(monitor, val));
        if (recursionTermination.isPresent())
            return recursionTermination.get();
        Class<?> childFieldCls = childFieldClsSupplier.get();
        if (childFieldCls.isPrimitive())
            return new GridToStringValueNode(childPropName, val);
        else if (childFieldCls.isArray())
            return new GridToStringArrayNode(childPropName, val, childFieldCls);
        else if (val instanceof Collection)
            return new GridToStringCollectionNode(childPropName, (Collection<?>)val);
        else if (val instanceof Map)
            return new GridToStringMapNode(childPropName, (Map<?, ?>)val);

        String toStrResult = val.toString();
        GridToStringNode node = recoverOrCreate(childPropName, toStrResult);
        node.innerBuf = INNER_BUILDER.get();
        INNER_BUILDER.set(null);
        return node;
    }

    /**
     * Determines if a property should be excluded from the output based on its sensitivity.
     * Checks if the property is marked as sensitive, or it's class marked as sensitive.
     * @param addVals The array of property values.
     * @param addSens The array of sensitivity flags.
     * @param idx The index of the property to check.
     * @return True if the property should be excluded; false otherwise.
     */
    private static boolean shouldBeExcluded(Object[] addVals, boolean[] addSens, int idx) {
        boolean fieldMarkedAsSensitive = addSens != null && addSens[idx];
        return fieldMarkedAsSensitive || Optional.ofNullable(addVals[idx])
                        .map(Object::getClass)
                        .map(cls -> cls.getAnnotation(GridToStringInclude.class))
                        .filter(GridToStringInclude::sensitive)
                        .isPresent();
    }

    /**
     * Tries to recover node from cached results or creates new,
     * if {@link GridToStringBuilder#toString)} is not called to get candidate
     * @param propName Property name.
     * @param candidate Marker candidate to recover already computed node.
     * @return Recovered node or new node with candidate's value
     */
    private static GridToStringNode recoverOrCreate(String propName, Object candidate) {
        return identities()
                .map(cache -> cache.remove(candidate))
                .map(node -> {
                    node.propName = propName;
                    if (node.previouslyCalculatedResult != null)
                        node.previouslyCalculatedResult = propName + "=" + node.previouslyCalculatedResult;
                    return node;
                })
                .orElseGet(() -> new GridToStringValueNode(propName, candidate));
    }
}
