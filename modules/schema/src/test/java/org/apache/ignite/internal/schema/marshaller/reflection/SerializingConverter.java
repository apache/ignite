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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.table.mapper.TypeConverter;

/**
 * Serializes an arbitrary user object (that extends Serializable) into a byte[] before write to the column, and deserializes back after
 * read.
 */
public class SerializingConverter<T> implements TypeConverter<T, byte[]> {
    /** {@inheritDoc} */
    @Override
    public byte[] toColumnType(T obj) throws Exception {
        if (obj == null) {
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(512);

        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(obj);
        }

        return out.toByteArray();
    }

    /** {@inheritDoc} */
    @Override
    public T toObjectType(byte[] data) throws Exception {
        if (data == null) {
            return null;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T) ois.readObject();
        }
    }
}
