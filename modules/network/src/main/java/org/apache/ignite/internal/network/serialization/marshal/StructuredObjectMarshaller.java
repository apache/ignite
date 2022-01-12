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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.network.serialization.SpecialMethodInvocationException;

/**
 * (Un)marshals objects that have structure (fields). These are {@link java.io.Serializable}s
 * (which are not {@link java.io.Externalizable}s) and arbitrary (non-serializable, non-externalizable) objects.
 */
class StructuredObjectMarshaller implements DefaultFieldsReaderWriter {
    private final TypedValueWriter valueWriter;
    private final ValueReader<Object> valueReader;

    private final Instantiation instantiation;

    StructuredObjectMarshaller(TypedValueWriter valueWriter, ValueReader<Object> valueReader) {
        this.valueWriter = valueWriter;
        this.valueReader = valueReader;

        instantiation = new BestEffortInstantiation(
                new SerializableInstantiation(),
                new UnsafeInstantiation()
        );
    }

    void writeStructuredObject(Object object, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException {
        for (ClassDescriptor layer : lineage(descriptor)) {
            writeStructuredObjectLayer(object, layer, output, context);
        }
    }

    /**
     * Returns the lineage (all the ancestors, from the progenitor (excluding Object) down the line, including the given class).
     *
     * @param descriptor class from which to obtain lineage
     * @return ancestors from the progenitor (excluding Object) down the line, plus the given class itself
     */
    private List<ClassDescriptor> lineage(ClassDescriptor descriptor) {
        List<ClassDescriptor> descriptors = new ArrayList<>();

        ClassDescriptor currentDesc = descriptor;
        while (currentDesc != null) {
            descriptors.add(currentDesc);
            currentDesc = currentDesc.superClassDescriptor();
        }

        Collections.reverse(descriptors);

        return descriptors;
    }

    private void writeStructuredObjectLayer(Object object, ClassDescriptor layer, DataOutputStream output, MarshallingContext context)
            throws IOException, MarshalException {
        if (layer.hasSerializationOverride()) {
            writeWithWriteObject(object, layer, output, context);
        } else {
            defaultWriteFields(object, layer, output, context);
        }

        context.addUsedDescriptor(layer);
    }

    private void writeWithWriteObject(Object object, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws IOException, MarshalException {
        context.startWritingWithWriteObject(object, descriptor);

        try {
            // Do not close the stream yet!
            UosObjectOutputStream oos = context.objectOutputStream(output, valueWriter, this);
            descriptor.serializationMethods().writeObject(object, oos);
            oos.flush();
        } catch (SpecialMethodInvocationException e) {
            throw new MarshalException("Cannot invoke writeObject()", e);
        } finally {
            context.endWritingWithWriteObject();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void defaultWriteFields(Object object, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException {
        for (FieldDescriptor fieldDescriptor : descriptor.fields()) {
            writeField(object, fieldDescriptor, output, context);
        }
    }

    private void writeField(Object object, FieldDescriptor fieldDescriptor, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException {
        Object fieldValue = fieldDescriptor.accessor().get(object);

        valueWriter.write(fieldValue, fieldDescriptor.clazz(), output, context);
    }

    Object preInstantiateStructuredObject(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return instantiation.newInstance(descriptor.clazz());
        } catch (InstantiationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.clazz(), e);
        }
    }

    void fillStructuredObjectFrom(DataInputStream input, Object object, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        for (ClassDescriptor layer : lineage(descriptor)) {
            fillStructuredObjectLayerFrom(input, layer, object, context);
        }
    }

    private void fillStructuredObjectLayerFrom(DataInputStream input, ClassDescriptor layer, Object object, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (layer.hasSerializationOverride()) {
            fillObjectWithReadObjectFrom(input, object, layer, context);
        } else {
            defaultFillFieldsFrom(input, object, layer, context);
        }
    }

    private void fillObjectWithReadObjectFrom(
            DataInputStream input,
            Object object,
            ClassDescriptor descriptor,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        context.startReadingWithReadObject(object, descriptor);

        try {
            // Do not close the stream yet!
            ObjectInputStream ois = context.objectInputStream(input, valueReader, this);
            descriptor.serializationMethods().readObject(object, ois);
        } catch (SpecialMethodInvocationException e) {
            throw new UnmarshalException("Cannot invoke readObject()", e);
        } finally {
            context.endReadingWithReadObject();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void defaultFillFieldsFrom(DataInputStream input, Object object, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        for (FieldDescriptor fieldDescriptor : descriptor.fields()) {
            Object fieldValue = valueReader.read(input, context);
            setFieldValue(object, fieldDescriptor, fieldValue);
        }
    }

    private void setFieldValue(Object target, FieldDescriptor fieldDescriptor, Object value) {
        fieldDescriptor.accessor().set(target, value);
    }

}
