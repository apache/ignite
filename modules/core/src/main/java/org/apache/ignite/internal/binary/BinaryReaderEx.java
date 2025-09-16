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

package org.apache.ignite.internal.binary;

import java.io.ObjectInput;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.jetbrains.annotations.Nullable;

/**
 * Extended reader interface.
 */
public interface BinaryReaderEx extends BinaryReader, BinaryRawReader, BinaryReaderHandlesHolder, ObjectInput {
    /**
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable public Object readObjectDetached() throws BinaryObjectException;

    /**
     * @param deserialize {@code True} if object should be deserialized during reading.
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable public Object readObjectDetached(boolean deserialize) throws BinaryObjectException;

    /**
     * @return Input stream.
     */
    public BinaryInputStream in();

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshal(int offset);

    /**
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    public Object deserialize() throws BinaryObjectException;

    /**
     * @return Descriptor.
     */
    public BinaryClassDescriptor descriptor();

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshalField(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldId Field ID.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshalField(int fieldId) throws BinaryObjectException;

    /**
     * Try finding the field by name.
     *
     * @param name Field name.
     * @return Offset.
     */
    public boolean findFieldByName(String name);
}
