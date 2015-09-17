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
import org.apache.ignite.internal.portable.api.PortableException;
import org.apache.ignite.internal.portable.api.PortableInvalidClassException;

/**
 *
 */
class PortableEnumArrayLazyValue extends PortableAbstractLazyValue {
    /** */
    private final int len;

    /** */
    private final int compTypeId;

    /** */
    private final String clsName;

    /**
     * @param reader Reader.
     */
    protected PortableEnumArrayLazyValue(PortableBuilderReader reader) {
        super(reader, reader.position() - 1);

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

            compTypeId = reader.portableContext().descriptorForClass(cls).typeId();
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

        PortableBuilderEnum[] res = new PortableBuilderEnum[size];

        for (int i = 0; i < size; i++) {
            byte flag = reader.readByte();

            if (flag == GridPortableMarshaller.NULL)
                continue;

            if (flag != GridPortableMarshaller.ENUM)
                throw new PortableException("Invalid flag value: " + flag);

            res[i] = new PortableBuilderEnum(reader);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (val != null) {
            if (clsName != null)
                ctx.writeArray(writer, GridPortableMarshaller.ENUM_ARR, (Object[])val, clsName);
            else
                ctx.writeArray(writer, GridPortableMarshaller.ENUM_ARR, (Object[])val, compTypeId);

            return;
        }

        writer.write(reader.array(), valOff, len);
    }
}