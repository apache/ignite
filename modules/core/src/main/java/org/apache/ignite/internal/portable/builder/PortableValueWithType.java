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

import org.apache.ignite.internal.portable.PortableWriterExImpl;
import org.apache.ignite.internal.processors.cache.portable.CacheObjectPortableProcessorImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class PortableValueWithType implements PortableLazyValue {
    /** */
    private byte type;

    /** */
    private Object val;

    /**
     * @param type Type
     * @param val Value.
     */
    PortableValueWithType(byte type, Object val) {
        this.type = type;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (val instanceof PortableBuilderSerializationAware)
            ((PortableBuilderSerializationAware)val).writeTo(writer, ctx);
        else
            ctx.writeValue(writer, val);
    }

    /** {@inheritDoc} */
    public String typeName() {
        return CacheObjectPortableProcessorImpl.fieldTypeName(type);
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        if (val instanceof PortableLazyValue)
            return ((PortableLazyValue)val).value();

        return val;
    }

    /**
     * @param val New value.
     */
    public void value(Object val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableValueWithType.class, this);
    }
}