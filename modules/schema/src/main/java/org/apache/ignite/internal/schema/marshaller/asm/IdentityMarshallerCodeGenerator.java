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

package org.apache.ignite.internal.schema.marshaller.asm;

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import org.apache.ignite.internal.schema.marshaller.Serializer;

/**
 * Generate {@link Serializer} method's bodies for simple types.
 */
class IdentityMarshallerCodeGenerator implements MarshallerCodeGenerator {
    /** Object field access expression generator. */
    private final TupleColumnAccessCodeGenerator columnAccessor;

    /** Target class. */
    private final Class<?> tClass;

    /**
     * Constructor.
     *
     * @param tClass Target class.
     * @param columnAccessor Tuple column code generator.
     */
    IdentityMarshallerCodeGenerator(Class<?> tClass, TupleColumnAccessCodeGenerator columnAccessor) {
        this.tClass = tClass;
        this.columnAccessor = columnAccessor;
    }

    /** {@inheritDoc} */
    @Override public boolean isSimpleType() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Class<?> targetClass() {
        return tClass;
    }

    /** {@inheritDoc} */
    @Override public BytecodeNode getValue(ParameterizedType type, Variable key, int i) {
        return key;
    }

    /** {@inheritDoc} */
    @Override public BytecodeNode marshallObject(ParameterizedType serializerClass, Variable asm, Variable obj) {
        return asm.invoke(columnAccessor.writeMethodName(), void.class, obj.cast(columnAccessor.writeArgType()));
    }

    /** {@inheritDoc} */
    @Override public BytecodeNode unmarshallObject(ParameterizedType type, Variable tuple, Variable obj) {
        return obj.set(
            tuple.invoke(
                columnAccessor.readMethodName(),
                columnAccessor.mappedType(),
                BytecodeExpressions.constantInt(columnAccessor.columnIdx())
            )
        );
    }
}
