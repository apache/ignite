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

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Generates {@link Serializer} methods code.
 */
class ObjectMarshallerCodeGenerator implements MarshallerCodeGenerator {
    /** Target class. */
    private final Class<?> targetClass;

    /** Mapped columns. */
    private final Columns columns;

    /** Object field access expression generators. */
    private final ColumnAccessCodeGenerator[] columnAccessors;

    ObjectMarshallerCodeGenerator(
            Columns columns,
            Class<?> targetClass,
            int firstColIdx
    ) {
        this.columns = columns;
        this.targetClass = targetClass;
        columnAccessors = new ColumnAccessCodeGenerator[columns.length()];

        try {
            for (int i = 0; i < columns.length(); i++) {
                final Field field = targetClass.getDeclaredField(columns.column(i).name());

                columnAccessors[i] = ColumnAccessCodeGenerator.createAccessor(MarshallerUtil.mode(field.getType()), i + firstColIdx);
            }
        } catch (NoSuchFieldException ex) {
            throw new IgniteInternalException(ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSimpleType() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> targetClass() {
        return targetClass;
    }

    /** {@inheritDoc} */
    @Override
    public BytecodeNode getValue(ParameterizedType serializerClass, Variable obj,
            int i) {
        final ColumnAccessCodeGenerator columnAccessor = columnAccessors[i];

        return BytecodeExpressions.getStatic(serializerClass, "FIELD_HANDLER_" + columnAccessor.columnIdx(),
                        ParameterizedType.type(VarHandle.class))
                .invoke("get", columnAccessor.mappedType(), obj);
    }

    /** {@inheritDoc} */
    @Override
    public BytecodeBlock marshallObject(ParameterizedType serializerClass, Variable asm, Variable obj) {
        final BytecodeBlock block = new BytecodeBlock();

        for (int i = 0; i < columns.length(); i++) {
            final ColumnAccessCodeGenerator columnAccessor = columnAccessors[i];

            final BytecodeExpression fld = BytecodeExpressions.getStatic(serializerClass, "FIELD_HANDLER_" + columnAccessor.columnIdx(),
                            ParameterizedType.type(VarHandle.class))
                    .invoke("get", columnAccessor.mappedType(), obj);

            final BytecodeExpression marshallNonNulExpr = asm.invoke(
                    columnAccessor.writeMethodName(),
                    RowAssembler.class,
                    Collections.singletonList(columnAccessor.writeArgType()),
                    fld.cast(columnAccessor.writeArgType()));

            if (columns.column(i).nullable()) {
                block.append(new BytecodeBlock().append(
                        new IfStatement().condition(BytecodeExpressions.isNull(fld))
                                .ifTrue(asm.invoke("appendNull", RowAssembler.class))
                                .ifFalse(marshallNonNulExpr))
                );
            } else {
                block.append(marshallNonNulExpr);
            }
        }

        return block;
    }

    /** {@inheritDoc} */
    @Override
    public BytecodeBlock unmarshallObject(ParameterizedType serializerClass, Variable row, Variable obj) {
        final BytecodeBlock block = new BytecodeBlock();

        for (int i = 0; i < columns.length(); i++) {
            final ColumnAccessCodeGenerator columnAccessor = columnAccessors[i];

            final BytecodeExpression val = row.invoke(
                    columnAccessor.readMethodName(),
                    columnAccessor.mappedType(),
                    BytecodeExpressions.constantInt(columnAccessor.columnIdx())
            );

            block.append(BytecodeExpressions.getStatic(serializerClass, "FIELD_HANDLER_" + columnAccessor.columnIdx(),
                            ParameterizedType.type(VarHandle.class))
                    .invoke("set", void.class, obj, val)
            );
        }

        return block;
    }

    /** {@inheritDoc} */
    @Override
    public void initStaticHandlers(ClassDefinition classDef, FieldDefinition targetClassField) {
        final MethodDefinition init = classDef.getClassInitializer();
        final Variable lookup = init.getScope().createTempVariable(MethodHandles.Lookup.class);

        final BytecodeBlock body = init.getBody().append(
                BytecodeExpressions.setStatic(
                targetClassField,
                        BytecodeExpressions.invokeStatic(Class.class, "forName", Class.class,
                                BytecodeExpressions.constantString(targetClass.getName()))
                ));

        if (isSimpleType()) {
            return;
        }

        body.append(
                lookup.set(
                        BytecodeExpressions.invokeStatic(
                                MethodHandles.class,
                                "privateLookupIn",
                                MethodHandles.Lookup.class,
                                BytecodeExpressions.getStatic(targetClassField),
                                BytecodeExpressions.invokeStatic(MethodHandles.class, "lookup", MethodHandles.Lookup.class))
                ));

        for (int i = 0; i < columnAccessors.length; i++) {
            final FieldDefinition fld = classDef.declareField(EnumSet.of(Access.PRIVATE, Access.STATIC, Access.FINAL),
                    "FIELD_HANDLER_" + columnAccessors[i].columnIdx(), VarHandle.class);

            body.append(
                    BytecodeExpressions.setStatic(fld, lookup.invoke(
                            "findVarHandle",
                            VarHandle.class,
                            BytecodeExpressions.getStatic(targetClassField),
                            BytecodeExpressions.constantString(columns.column(i).name()),
                            BytecodeExpressions.constantClass(columnAccessors[i].mappedType())
                    ))
            );
        }
    }
}
