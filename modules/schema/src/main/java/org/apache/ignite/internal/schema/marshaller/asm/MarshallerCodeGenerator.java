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
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;

/**
 * Marshaller code generator.
 */
public interface MarshallerCodeGenerator {
    /**
     * @return {@code true} if it is simple object marshaller, {@code false} otherwise.
     */
    boolean isSimpleType();

    /**
     * @return Target class.
     */
    Class<?> targetClass();

    /**
     * @param serializerClass Serializer type.
     * @param obj Target object variable.
     * @param colIdx Column index.
     * @return Object field value for given column.
     */
    BytecodeNode getValue(ParameterizedType serializerClass, Variable obj, int colIdx);

    /**
     * @param serializerClass Serializer type
     * @param asm Tuple assembler.
     * @param obj Target object variable.
     * @return Unmarshall object code.
     */
    BytecodeNode marshallObject(ParameterizedType serializerClass, Variable asm, Variable obj);

    /**
     * @param serializerClass Serializer type
     * @param tuple Tuple.
     * @param obj Result object variable.
     * @return Unmarshall object code.
     */
    BytecodeNode unmarshallObject(ParameterizedType serializerClass, Variable tuple, Variable obj);

    /**
     * @param classDef Class definition.
     * @param tClassField Target class field definition.
     */
    default void initStaticHandlers(ClassDefinition classDef, FieldDefinition tClassField) {

    }
}
