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

package org.apache.ignite.internal.portable;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.portable.*;

/**
 *
 */
class PortableObjectArrayLazyValue extends PortableAbstractLazyValue {
    /** */
    private Object[] lazyValsArr;

    /** */
    private int compTypeId;

    /** */
    private String clsName;

    /**
     * @param reader Reader.
     */
    protected PortableObjectArrayLazyValue(PortableBuilderReader reader) {
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

        lazyValsArr = new Object[size];

        for (int i = 0; i < size; i++)
            lazyValsArr[i] = reader.parseValue();
    }

    /** {@inheritDoc} */
    @Override protected Object init() {
        for (int i = 0; i < lazyValsArr.length; i++) {
            if (lazyValsArr[i] instanceof PortableLazyValue)
                lazyValsArr[i] = ((PortableLazyValue)lazyValsArr[i]).value();
        }

        return lazyValsArr;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (clsName == null)
            ctx.writeArray(writer, GridPortableMarshaller.OBJ_ARR, lazyValsArr, compTypeId);
        else
            ctx.writeArray(writer, GridPortableMarshaller.OBJ_ARR, lazyValsArr, clsName);
    }
}
