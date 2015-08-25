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

import java.util.*;

/**
 *
 */
class PortableLazySet extends PortableAbstractLazyValue {
    /** */
    private final int off;

    /**
     * @param reader Reader.
     * @param size Size.
     */
    PortableLazySet(PortableBuilderReader reader, int size) {
        super(reader, reader.position() - 1);

        off = reader.position() - 1/* flag */ - 4/* size */ - 1/* col type */;

        assert size >= 0;

        for (int i = 0; i < size; i++)
            reader.skipValue();
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (val == null) {
            int size = reader.readIntAbsolute(off + 1);

            int hdrSize = 1 /* flag */ + 4 /* size */ + 1 /* col type */;
            writer.write(reader.array(), off, hdrSize);

            reader.position(off + hdrSize);

            for (int i = 0; i < size; i++) {
                Object o = reader.parseValue();

                ctx.writeValue(writer, o);
            }
        }
        else {
            Collection<Object> c = (Collection<Object>)val;

            writer.writeByte(GridPortableMarshaller.COL);
            writer.writeInt(c.size());

            byte colType = reader.array()[off + 1 /* flag */ + 4 /* size */];
            writer.writeByte(colType);

            for (Object o : c)
                ctx.writeValue(writer, o);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object init() {
        int size = reader.readIntAbsolute(off + 1);

        reader.position(off + 1/* flag */ + 4/* size */ + 1/* col type */);

        Set<Object> res = U.newLinkedHashSet(size);

        for (int i = 0; i < size; i++)
            res.add(PortableUtils.unwrapLazy(reader.parseValue()));

        return res;
    }
}
