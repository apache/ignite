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
     * Returns a code that access a field value of the {@code obj} for an associated column with given index.
     *
     * @param marshallerClass Marshaller class.
     * @param obj             Target object variable.
     * @param colIdx          Column index.
     * @return Object field value for given column.
     */
    BytecodeNode getValue(ParameterizedType marshallerClass, Variable obj, int colIdx);

    /**
     * Returns a code that marshal the {@code obj} using the {@code assembler}.
     *
     * @param marshallerClass Marshaller class.
     * @param assembler       Row assembler.
     * @param obj             Target object variable.
     * @return Unmarshall object code.
     */
    BytecodeNode marshallObject(ParameterizedType marshallerClass, Variable assembler, Variable obj);

    /**
     * Returns a code that unmarshal key-part of a {@code row} to an {@code obj}.
     *
     * @param marshallerClass Marshaller class.
     * @param row             Row.
     * @param obj             Result object variable.
     * @param objFactory      Object factory variable.
     * @return Unmarshall object code.
     */
    BytecodeNode unmarshallObject(ParameterizedType marshallerClass, Variable row, Variable obj, Variable objFactory);

    /**
     * Initialize static VarHandle instances for accessing a target class, which the {@code targetClassField} holds.
     *
     * @param classDef         Class definition.
     * @param targetClassField Target class field definition.
     */
    default void initStaticHandlers(ClassDefinition classDef, FieldDefinition targetClassField) {

    }
}
