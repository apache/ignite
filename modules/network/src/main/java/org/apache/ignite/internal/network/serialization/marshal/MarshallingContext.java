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

import static java.util.Collections.unmodifiableSet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.NotActiveException;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * Context using during marshalling of an object graph accessible from a root object.
 */
class MarshallingContext {
    private final Set<ClassDescriptor> usedDescriptors = new HashSet<>();

    private final Map<Object, Integer> objectsToIds = new IdentityHashMap<>();

    private int nextObjectId = 0;

    private Object objectCurrentlyWrittenWithWriteObject;
    private ClassDescriptor descriptorOfObjectCurrentlyWrittenWithWriteObject;

    private UosObjectOutputStream objectOutputStream;

    public void addUsedDescriptor(ClassDescriptor descriptor) {
        usedDescriptors.add(descriptor);
    }

    public Set<ClassDescriptor> usedDescriptors() {
        return unmodifiableSet(usedDescriptors);
    }

    /**
     * Memorizes the object and returns an object ID (with flags) for this object. The returned value is a long that should be
     * handled using {@link FlaggedObjectIds}.
     * If {@code unshared} is {@code true}, a fresh object ID is generated and the object is not memorized.
     * If {@code unshared} is {@code false}: if the object was already seen before, its previous ID is returned; otherwise,
     * it's memorized as seen with a fresh ID which is returned.
     *
     * @param object object to operate upon ({@code null} is not supported)
     * @param unshared if this is {@code true}, then new object ID is generated even if the object was already seen before
     * @return object ID with flags (use {@link FlaggedObjectIds} to work with this value)
     */
    public long memorizeObject(Object object, boolean unshared) {
        Objects.requireNonNull(object);

        if (unshared) {
            int newId = nextId();
            return FlaggedObjectIds.freshObjectId(newId);
        }

        Integer prevId = objectsToIds.get(object);
        if (prevId != null) {
            return FlaggedObjectIds.alreadySeenObjectId(prevId);
        } else {
            int newId = nextId();

            objectsToIds.put(object, newId);

            return FlaggedObjectIds.freshObjectId(newId);
        }
    }

    private int nextId() {
        return nextObjectId++;
    }

    public Object objectCurrentlyWrittenWithWriteObject() throws NotActiveException {
        if (objectCurrentlyWrittenWithWriteObject == null) {
            throw new NotActiveException("not in call to writeObject");
        }

        return objectCurrentlyWrittenWithWriteObject;
    }

    public ClassDescriptor descriptorOfObjectCurrentlyWrittenWithWriteObject() {
        if (descriptorOfObjectCurrentlyWrittenWithWriteObject == null) {
            throw new IllegalStateException("No object is currently being written");
        }

        return descriptorOfObjectCurrentlyWrittenWithWriteObject;
    }

    public void startWritingWithWriteObject(Object object, ClassDescriptor descriptor) {
        objectCurrentlyWrittenWithWriteObject = object;
        descriptorOfObjectCurrentlyWrittenWithWriteObject = descriptor;
    }

    public void endWritingWithWriteObject() {
        objectCurrentlyWrittenWithWriteObject = null;
        descriptorOfObjectCurrentlyWrittenWithWriteObject = null;
    }

    UosObjectOutputStream objectOutputStream(
            DataOutputStream output,
            TypedValueWriter valueWriter,
            TypedValueWriter unsharedWriter,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter
    ) throws IOException {
        if (objectOutputStream == null) {
            objectOutputStream = new UosObjectOutputStream(output, valueWriter, unsharedWriter, defaultFieldsReaderWriter, this);
        }

        return objectOutputStream;
    }
}
