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
 * Schema serializer.
 */
public abstract class AbstractSchemaSerializer implements SchemaSerializer {
    /** Schema serializer version. */
    protected final short version;

    /** Previous version serializer. */
    protected final AbstractSchemaSerializer previous;

    /**
     * @param ver      Serializer version.
     * @param previous Previous version serializer.
     */
    protected AbstractSchemaSerializer(short ver, AbstractSchemaSerializer previous) {
        this.version = ver;
        this.previous = previous;
    }

    /**
     * @param ver Serializer version.
     */
    protected AbstractSchemaSerializer(short ver) {
        this(ver, null);
    }

    /**
     * @return Serializer version;
     */
    public short getVersion() {
        return version;
    }

    /**
     * Serialize SchemaDescriptor object to byte array.
     *
     * @param desc SchemaDescriptor object.
     * @return SchemaDescriptor byte array representation.
     */
    public byte[] serialize(SchemaDescriptor desc) {
        ByteBuffer buf = ByteBuffer.allocate(size(desc));

        this.writeTo(desc, buf);

        return buf.array();
    }

    /**
     * Deserialize byte array to SchemaDescriptor object.
     *
     * @param bytes SchemaDescriptor byte array representation.
     * @return SchemaDescriptor object.
     */
    public SchemaDescriptor deserialize(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        short ver = readVersion(buf);

        return getSerializerByVersion(ver).readFrom(buf);
    }

    /**
     * Gets schema serializer by version.
     *
     * @param ver SchemaSerializer target version.
     * @return SchemaSerializer object.
     * @throws IllegalArgumentException If SchemaSerializer with right version is not found.
     */
    private SchemaSerializer getSerializerByVersion(short ver) {
        if (ver == this.version) {
            return this;
        } else if (this.previous == null) {
            throw new IllegalArgumentException("Unable to find schema serializer with version " + ver);
        }

        return this.previous.getSerializerByVersion(ver);
    }

    /**
     * Reads SchemaSerializer version from byte buffer.
     *
     * @param buf ByteBuffer object.
     * @return SchemaSerializer version.
     */
    private short readVersion(ByteBuffer buf) {
        return buf.getShort();
    }
}
