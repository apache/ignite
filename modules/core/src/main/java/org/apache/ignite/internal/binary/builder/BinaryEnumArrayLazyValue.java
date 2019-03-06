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

import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryInvalidTypeException;

/**
 *
 */
class BinaryEnumArrayLazyValue extends BinaryAbstractLazyValue {
    /** */
    private final int len;

    /** */
    private final int compTypeId;

    /** */
    private final String clsName;

    /**
     * @param reader Reader.
     */
    protected BinaryEnumArrayLazyValue(BinaryBuilderReader reader) {
        super(reader, reader.position() - 1);

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

            compTypeId = reader.binaryContext().descriptorForClass(cls, true, false).typeId();
        }
        else {
            compTypeId = typeId;
            clsName = null;
        }

        int size = reader.readInt();

        for (int i = 0; i < size; i++)
            reader.skipValue();

        len = reader.position() - valOff;
    }

    /** {@inheritDoc} */
    @Override protected Object init() {
        reader.position(valOff + 1);

        //skipping component type id
        reader.readInt();

        int size = reader.readInt();

        BinaryBuilderEnum[] res = new BinaryBuilderEnum[size];

        for (int i = 0; i < size; i++) {
            byte flag = reader.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                continue;

            if (flag != GridBinaryMarshaller.ENUM)
                throw new BinaryObjectException("Invalid flag value: " + flag);

            res[i] = new BinaryBuilderEnum(reader);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
        if (val != null) {
            if (clsName != null)
                ctx.writeArray(writer, GridBinaryMarshaller.ENUM_ARR, (Object[])val, clsName);
            else
                ctx.writeArray(writer, GridBinaryMarshaller.ENUM_ARR, (Object[])val, compTypeId);

            return;
        }

        writer.write(reader.array(), valOff, len);
    }
}
