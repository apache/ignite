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

package org.apache.ignite.internal.schema.marshaller.schema;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * SchemaDescriptor (De)Serializer interface.
 */
public interface SchemaSerializer {
    /**
     * Writes SchemaDescriptor object to byte buffer.
     *
     * @param desc    SchemaDescriptor object.
     * @param byteBuf ByteBuffer object with allocated byte array.
     */
    void writeTo(SchemaDescriptor desc, ByteBuffer byteBuf);

    /**
     * @param byteBuf Byte buffer with byte array.
     * @return SchemaDescriptor object.
     */
    SchemaDescriptor readFrom(ByteBuffer byteBuf);

    /**
     * Calculates size in bytes of SchemaDescriptor object.
     *
     * @param desc SchemaDescriptor object.
     * @return size in bytes.
     */
    int size(SchemaDescriptor desc);
}
