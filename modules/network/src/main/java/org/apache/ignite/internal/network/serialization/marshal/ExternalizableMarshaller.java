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
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * (Um)marshalling specific to EXTERNALIZABLE serialization type.
 */
class ExternalizableMarshaller {
    private final NoArgConstructorInstantiation instantiation = new NoArgConstructorInstantiation();

    void writeExternalizable(Externalizable externalizable, ClassDescriptor descriptor, DataOutput output, MarshallingContext context)
            throws IOException {
        byte[] externalizableBytes = externalize(externalizable);

        output.writeInt(externalizableBytes.length);
        output.write(externalizableBytes);

        context.addUsedDescriptor(descriptor);
    }

    private byte[] externalize(Externalizable externalizable) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            externalizable.writeExternal(oos);
        }

        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    <T extends Externalizable> T preInstantiateExternalizable(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return (T) instantiation.newInstance(descriptor.clazz());
        } catch (InstantiationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.clazz(), e);
        }
    }

    <T extends Externalizable> void fillExternalizableFrom(DataInput input, T object) throws IOException, UnmarshalException {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readFully(bytes);

        try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            object.readExternal(ois);
        } catch (ClassNotFoundException e) {
            throw new UnmarshalException("Cannot unmarshal due to a missing class", e);
        }
    }
}
