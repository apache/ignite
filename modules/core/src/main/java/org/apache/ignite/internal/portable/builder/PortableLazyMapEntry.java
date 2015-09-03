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

import org.apache.ignite.internal.portable.*;

import java.util.Map;

/**
 *
 */
class PortableLazyMapEntry implements Map.Entry<Object, Object>, PortableBuilderSerializationAware {
    /** */
    private final Object key;

    /** */
    private Object val;

    /**
     * @param reader GridMutablePortableReader
     */
    PortableLazyMapEntry(PortableBuilderReader reader) {
        key = reader.parseValue();
        val = reader.parseValue();
    }

    /** {@inheritDoc} */
    @Override public Object getKey() {
        return PortableUtils.unwrapLazy(key);
    }

    /** {@inheritDoc} */
    @Override public Object getValue() {
        return PortableUtils.unwrapLazy(val);
    }

    /** {@inheritDoc} */
    @Override public Object setValue(Object val) {
        Object res = getValue();

        this.val = val;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        writer.writeByte(GridPortableMarshaller.MAP_ENTRY);

        ctx.writeValue(writer, key);
        ctx.writeValue(writer, val);
    }
}
