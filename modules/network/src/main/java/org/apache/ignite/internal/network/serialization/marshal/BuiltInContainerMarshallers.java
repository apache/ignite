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

package org.apache.ignite.internal.network.serialization.marshal;

import static java.util.Collections.singletonList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * Utility to (un)marshal built-in collections and maps.
 */
class BuiltInContainerMarshallers {
    /**
     * Map of all classes which are built-in collections AND may have different sizes AND are mutable. This makes
     * them eligible for a generic unmarshal algorithm: read length, create an empty collection, then read N elements
     * and add each of them into the collection.
     */
    private final Map<Class<?>, IntFunction<? extends Collection<?>>> mutableBuiltInCollectionFactories = Map.of(
            ArrayList.class, ArrayList::new,
            LinkedList.class, len -> new LinkedList<>(),
            HashSet.class, HashSet::new,
            LinkedHashSet.class, LinkedHashSet::new
    );

    /**
     * Map of all classes which are built-in maps AND may have different sizes AND are mutable. This makes
     * them eligible for a generic unmarshal algorithm: read length, create an empty map, then read N entries
     * and put each of them into the map.
     */
    private final Map<Class<?>, IntFunction<? extends Map<?, ?>>> mutableBuiltInMapFactories = Map.of(
            HashMap.class, HashMap::new,
            LinkedHashMap.class, LinkedHashMap::new
    );

    private final TrackingMarshaller trackingMarshaller;

    BuiltInContainerMarshallers(TrackingMarshaller trackingMarshaller) {
        this.trackingMarshaller = trackingMarshaller;
    }

    Set<ClassDescriptor> writeGenericRefArray(Object[] array, ClassDescriptor arrayDescriptor, DataOutput output)
            throws IOException, MarshalException {
        output.writeUTF(array.getClass().getComponentType().getName());
        return writeCollection(Arrays.asList(array), arrayDescriptor, output);
    }

    <T> T[] readGenericRefArray(DataInput input, ValueReader<T> elementReader, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        return BuiltInMarshalling.readGenericRefArray(input, elementReader, context);
    }

    Set<ClassDescriptor> writeBuiltInCollection(Collection<?> object, ClassDescriptor descriptor, DataOutput output)
            throws IOException, MarshalException {
        if (supportsAsMutableBuiltInCollection(descriptor)) {
            return writeCollection(object, descriptor, output);
        } else if (descriptor.isSingletonList()) {
            return writeSingletonList((List<?>) object, descriptor, output);
        } else {
            throw new IllegalStateException("Marshalling of " + descriptor.clazz() + " is not supported, but it's marked as a built-in");
        }
    }

    /**
     * Returns {@code true} if the given descriptor is supported as a built-in mutable collection. Such types
     * are eligible for a generic unmarshal algorithm: read length, create an empty collection, then read N elements
     * and add each of them into the collection.
     *
     * @param descriptor the descriptor to check
     * @return {@code true} if the given descriptor is supported as a built-in mutable collection
     */
    private boolean supportsAsMutableBuiltInCollection(ClassDescriptor descriptor) {
        return mutableBuiltInCollectionFactories.containsKey(descriptor.clazz());
    }

    private Set<ClassDescriptor> writeCollection(Collection<?> collection, ClassDescriptor collectionDescriptor, DataOutput output)
            throws IOException, MarshalException {
        Set<ClassDescriptor> usedDescriptors = new HashSet<>();
        usedDescriptors.add(collectionDescriptor);

        BuiltInMarshalling.writeCollection(collection, output, writerAddingUsedDescriptor(usedDescriptors));

        return usedDescriptors;
    }

    private <T> ValueWriter<T> writerAddingUsedDescriptor(Set<ClassDescriptor> usedDescriptors) {
        return (elem, out) -> {
            Set<ClassDescriptor> elementDescriptors = trackingMarshaller.marshal(elem, out);
            usedDescriptors.addAll(elementDescriptors);
        };
    }

    private Set<ClassDescriptor> writeSingletonList(List<?> list, ClassDescriptor listDescriptor, DataOutput output)
            throws MarshalException, IOException {
        assert list.size() == 1;

        Object element = list.get(0);

        Set<ClassDescriptor> usedDescriptors = new HashSet<>();
        usedDescriptors.add(listDescriptor);

        Set<ClassDescriptor> descriptorsFromElement = trackingMarshaller.marshal(element, output);
        usedDescriptors.addAll(descriptorsFromElement);

        return usedDescriptors;
    }

    @SuppressWarnings("unchecked")
    <T, C extends Collection<T>> C readBuiltInCollection(
            ClassDescriptor collectionDescriptor,
            ValueReader<T> elementReader,
            DataInput input,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        if (collectionDescriptor.isSingletonList()) {
            return (C) singletonList(elementReader.read(input, context));
        }

        IntFunction<C> collectionFactory = (IntFunction<C>) mutableBuiltInCollectionFactories.get(collectionDescriptor.clazz());

        if (collectionFactory == null) {
            throw new IllegalStateException("Did not find a collection factory for " + collectionDescriptor.clazz()
                    + " even though it is marked as a built-in");
        }

        return BuiltInMarshalling.readCollection(input, collectionFactory, elementReader, context);
    }

    Set<ClassDescriptor> writeBuiltInMap(Map<?, ?> map, ClassDescriptor mapDescriptor, DataOutput output)
            throws MarshalException, IOException {
        if (!supportsAsBuiltInMap(mapDescriptor)) {
            throw new IllegalStateException("Marshalling of " + mapDescriptor.clazz() + " is not supported, but it's marked as a built-in");
        }

        Set<ClassDescriptor> usedDescriptors = new HashSet<>();
        usedDescriptors.add(mapDescriptor);

        BuiltInMarshalling.writeMap(
                map,
                output,
                writerAddingUsedDescriptor(usedDescriptors),
                writerAddingUsedDescriptor(usedDescriptors)
        );

        return usedDescriptors;
    }

    private boolean supportsAsBuiltInMap(ClassDescriptor mapDescriptor) {
        return mutableBuiltInMapFactories.containsKey(mapDescriptor.clazz());
    }

    <K, V, M extends Map<K, V>> M readBuiltInMap(
            ClassDescriptor mapDescriptor,
            ValueReader<K> keyReader,
            ValueReader<V> valueReader,
            DataInput input,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        @SuppressWarnings("unchecked")
        IntFunction<M> mapFactory = (IntFunction<M>) mutableBuiltInMapFactories.get(mapDescriptor.clazz());

        if (mapFactory == null) {
            throw new IllegalStateException("Did not find a map factory for " + mapDescriptor.clazz()
                    + " even though it is marked as a built-in");
        }

        return BuiltInMarshalling.readMap(input, mapFactory, keyReader, valueReader, context);
    }
}
