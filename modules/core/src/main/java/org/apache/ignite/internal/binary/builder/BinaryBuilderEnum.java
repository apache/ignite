/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.binary.builder;

import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.binary.BinaryInvalidTypeException;

/**
 *
 */
public class BinaryBuilderEnum implements BinaryBuilderSerializationAware {
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
    public BinaryBuilderEnum(int typeId, Enum anEnum) {
        ordinal = anEnum.ordinal();
        this.typeId = typeId;
        clsName = null;
    }

    /**
     * @param reader BinaryBuilderReader.
     */
    public BinaryBuilderEnum(BinaryBuilderReader reader) {
        int typeId = reader.readInt();

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            clsName = reader.readString();

            Class cls;

            try {
                cls = U.forName(reader.readString(), reader.binaryContext().configuration().getClassLoader());
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            this.typeId = reader.binaryContext().descriptorForClass(cls, false, false).typeId();
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
    @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
        writer.writeByte(GridBinaryMarshaller.ENUM);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            writer.writeInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);
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

        BinaryBuilderEnum that = (BinaryBuilderEnum)o;

        return ordinal == that.ordinal && typeId == that.typeId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = ordinal;

        result = 31 * result + typeId;

        return result;
    }
}
