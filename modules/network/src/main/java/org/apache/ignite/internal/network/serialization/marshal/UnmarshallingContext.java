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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.NotActiveException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.jetbrains.annotations.Nullable;

/**
 * Context of unmarshalling act. Created once per unmarshalling a root object.
 */
class UnmarshallingContext implements IdIndexedDescriptors {
    private final ByteArrayInputStream source;
    private final IdIndexedDescriptors descriptors;

    private final Map<Integer, Object> idsToObjects = new HashMap<>();

    private Object objectCurrentlyReadWithReadObject;
    private ClassDescriptor descriptorOfObjectCurrentlyReadWithReadObject;

    private UosObjectInputStream objectInputStream;

    public UnmarshallingContext(ByteArrayInputStream source, IdIndexedDescriptors descriptors) {
        this.source = source;
        this.descriptors = descriptors;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ClassDescriptor getDescriptor(int descriptorId) {
        return descriptors.getDescriptor(descriptorId);
    }

    public void registerReference(int objectId, Object object) {
        idsToObjects.put(objectId, object);
    }

    @SuppressWarnings("unchecked")
    public <T> T dereference(int objectId) {
        Object result = idsToObjects.get(objectId);

        if (result == null) {
            throw new IllegalStateException("Unknown object ID: " + objectId);
        }

        return (T) result;
    }

    public void markSource(int readAheadLimit) {
        source.mark(readAheadLimit);
    }

    public void resetSourceToMark() {
        source.reset();
    }

    public Object objectCurrentlyReadWithReadObject() throws NotActiveException {
        if (objectCurrentlyReadWithReadObject == null) {
            throw new NotActiveException("not in call to readObject");
        }

        return objectCurrentlyReadWithReadObject;
    }

    public ClassDescriptor descriptorOfObjectCurrentlyReadWithReadObject() {
        if (descriptorOfObjectCurrentlyReadWithReadObject == null) {
            throw new IllegalStateException("No object is currently being read with readObject()");
        }

        return descriptorOfObjectCurrentlyReadWithReadObject;
    }

    public void startReadingWithReadObject(Object object, ClassDescriptor descriptor) {
        objectCurrentlyReadWithReadObject = object;
        descriptorOfObjectCurrentlyReadWithReadObject = descriptor;
    }

    public void endReadingWithReadObject() {
        objectCurrentlyReadWithReadObject = null;
        descriptorOfObjectCurrentlyReadWithReadObject = null;
    }

    UosObjectInputStream objectInputStream(
            DataInputStream input,
            ValueReader<Object> valueReader,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter
    ) throws IOException {
        if (objectInputStream == null) {
            objectInputStream = new UosObjectInputStream(input, valueReader, defaultFieldsReaderWriter, this);
        }

        return objectInputStream;
    }
}
