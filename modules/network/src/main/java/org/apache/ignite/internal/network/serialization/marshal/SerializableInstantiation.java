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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.ObjectStreamConstants;
import java.io.Serializable;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassIndexedDescriptors;

/**
 * Instantiates {@link Serializable} classes (they are the only ones supported) by crafting a representation of
 * a serialized object of a given class (without any field data) and then deserializing it using the standard
 * Java Serialization.
 */
class SerializableInstantiation implements Instantiation {

    private static final int STREAM_VERSION = 5;

    private final ClassIndexedDescriptors descriptors;

    SerializableInstantiation(ClassIndexedDescriptors descriptors) {
        this.descriptors = descriptors;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supports(Class<?> objectClass) {
        if (!Serializable.class.isAssignableFrom(objectClass)) {
            return false;
        }

        ClassDescriptor descriptor = descriptors.getRequiredDescriptor(objectClass);
        return !descriptor.hasWriteReplace() && !descriptor.hasReadResolve();
    }

    /** {@inheritDoc} */
    @Override
    public Object newInstance(Class<?> objectClass) throws InstantiationException {
        byte[] jdkSerialization = jdkSerializationOfEmptyInstanceOf(objectClass);

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(jdkSerialization))) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new InstantiationException("Cannot deserialize JDK serialization of an empty instance", e);
        }
    }

    private byte[] jdkSerializationOfEmptyInstanceOf(Class<?> objectClass) throws InstantiationException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (DataOutputStream dos = new DataOutputStream(baos)) {
            writeSignature(dos);

            dos.writeByte(ObjectStreamConstants.TC_OBJECT);
            dos.writeByte(ObjectStreamConstants.TC_CLASSDESC);
            dos.writeUTF(objectClass.getName());

            dos.writeLong(serialVersionUid(objectClass));

            writeFlags(dos);

            writeZeroFields(dos);

            dos.writeByte(ObjectStreamConstants.TC_ENDBLOCKDATA);
            writeNullForNoParentDescriptor(dos);
        } catch (IOException e) {
            throw new InstantiationException("Cannot create JDK serialization of an empty instance", e);
        }

        return baos.toByteArray();
    }

    private void writeSignature(DataOutputStream dos) throws IOException {
        dos.writeShort(ObjectStreamConstants.STREAM_MAGIC);
        dos.writeShort(STREAM_VERSION);
    }

    private long serialVersionUid(Class<?> objectClass) {
        ObjectStreamClass descriptor = ObjectStreamClass.lookup(objectClass);
        return descriptor.getSerialVersionUID();
    }

    private void writeFlags(DataOutputStream dos) throws IOException {
        dos.writeByte(ObjectStreamConstants.SC_SERIALIZABLE);
    }

    private void writeZeroFields(DataOutputStream dos) throws IOException {
        dos.writeShort(0);
    }

    private void writeNullForNoParentDescriptor(DataOutputStream dos) throws IOException {
        dos.writeByte(ObjectStreamConstants.TC_NULL);
    }
}
