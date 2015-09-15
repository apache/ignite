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

package org.apache.ignite.internal.portable.builder;

import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.portable.PortableInvalidClassException;

/**
 *
 */
public class PortableBuilderEnum implements PortableBuilderSerializationAware {
    /** */
    private final int ordinal;

    /** */
    private final int typeId;

    /** */
    private final String clsName;

    /**
     * @param typeId Type ID.
     * @param anEnum Enum instance.
     */
    public PortableBuilderEnum(int typeId, Enum anEnum) {
        ordinal = anEnum.ordinal();
        this.typeId = typeId;
        clsName = null;
    }

    /**
     * @param reader PortableBuilderReader.
     */
    public PortableBuilderEnum(PortableBuilderReader reader) {
        int typeId = reader.readInt();

        if (typeId == GridPortableMarshaller.UNREGISTERED_TYPE_ID) {
            clsName = reader.readString();

            Class cls;

            try {
                // TODO: IGNITE-1272 - Is class loader needed here?
                cls = U.forName(reader.readString(), null);
            }
            catch (ClassNotFoundException e) {
                throw new PortableInvalidClassException("Failed to load the class: " + clsName, e);
            }

            this.typeId = reader.portableContext().descriptorForClass(cls).typeId();
        }
        else {
            this.typeId = typeId;
            this.clsName = null;
        }

        ordinal = reader.readInt();
    }

    /**
     * @return Ordinal.
     */
    public int getOrdinal() {
        return ordinal;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        writer.writeByte(GridPortableMarshaller.ENUM);

        if (typeId == GridPortableMarshaller.UNREGISTERED_TYPE_ID) {
            writer.writeInt(GridPortableMarshaller.UNREGISTERED_TYPE_ID);
            writer.writeString(clsName);
        }
        else
            writer.writeInt(typeId);

        writer.writeInt(ordinal);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        PortableBuilderEnum that = (PortableBuilderEnum)o;

        return ordinal == that.ordinal && typeId == that.typeId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = ordinal;

        result = 31 * result + typeId;

        return result;
    }
}