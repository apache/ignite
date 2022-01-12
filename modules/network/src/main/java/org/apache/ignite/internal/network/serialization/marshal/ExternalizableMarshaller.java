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
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * (Um)marshalling specific to EXTERNALIZABLE serialization type.
 */
class ExternalizableMarshaller {
    private final ValueReader<Object> valueReader;
    private final TypedValueWriter typedValueWriter;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;

    private final NoArgConstructorInstantiation instantiation = new NoArgConstructorInstantiation();

    ExternalizableMarshaller(
            ValueReader<Object> valueReader,
            TypedValueWriter typedValueWriter,
            DefaultFieldsReaderWriter defaultFieldsReaderWriter
    ) {
        this.valueReader = valueReader;
        this.typedValueWriter = typedValueWriter;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
    }

    void writeExternalizable(Externalizable externalizable, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws IOException {
        externalizeTo(externalizable, output, context);

        context.addUsedDescriptor(descriptor);
    }

    private void externalizeTo(Externalizable externalizable, DataOutputStream output, MarshallingContext context)
            throws IOException {
        context.endWritingWithWriteObject();

        // Do not close the stream yet!
        UosObjectOutputStream oos = context.objectOutputStream(output, typedValueWriter, defaultFieldsReaderWriter);
        externalizable.writeExternal(oos);
        oos.flush();
    }

    @SuppressWarnings("unchecked")
    <T extends Externalizable> T preInstantiateExternalizable(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return (T) instantiation.newInstance(descriptor.clazz());
        } catch (InstantiationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.clazz(), e);
        }
    }

    <T extends Externalizable> void fillExternalizableFrom(DataInputStream input, T object, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        context.endReadingWithReadObject();

        // Do not close the stream yet!
        ObjectInputStream ois = context.objectInputStream(input, valueReader, defaultFieldsReaderWriter);
        try {
            object.readExternal(ois);
        } catch (ClassNotFoundException e) {
            throw new UnmarshalException("Cannot unmarshal due to a missing class", e);
        }
    }
}
