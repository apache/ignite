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

package org.apache.ignite.internal.configuration.asm;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromJavaClassName;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;
import static java.util.EnumSet.of;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.LAMBDA_METAFACTORY;
import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.internalName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInjectedName;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isInternalId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isNamedConfigValue;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isValue;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;
import static org.objectweb.asm.Type.VOID_TYPE;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.direct.DirectConfigurationProxy;
import org.apache.ignite.internal.configuration.direct.DirectNamedListProxy;
import org.apache.ignite.internal.configuration.direct.DirectValueProxy;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Handle;

/**
 * Helper class to generate classes that extend {@link DirectConfigurationProxy}.
 * All that's required here is to generate constructor and a bunch of getter methods.
 */
class DirectProxyAsmGenerator {
    /** {@link DirectConfigurationProxy#DirectConfigurationProxy(List, DynamicConfigurationChanger)}. */
    private static final Constructor<?> DIRECT_CFG_CTOR;

    /** {@link ConfigurationUtil#appendKey(List, Object)}. */
    private static final Method APPEND_KEY;

    /** This generator instance is only used for {@link ConfigurationAsmGenerator#schemaInfo(java.lang.Class)}. */
    private final ConfigurationAsmGenerator cgen;

    /** Schema class. */
    private final Class<?> schemaClass;

    /** Set of internal extensions. */
    private final Set<Class<?>> internalExtensions;

    /** Fields from the schema class. */
    private final List<Field> schemaFields;

    /** Fields from all internal extensions. */
    private final Collection<Field> internalExtensionsFields;

    /** {@link InternalId} field. */
    @Nullable
    private final Field internalIdField;

    /** Class definition that extends the {@link DirectConfigurationProxy}. */
    private ClassDefinition classDef;

    static {
        try {
            DIRECT_CFG_CTOR = DirectConfigurationProxy.class.getDeclaredConstructor(List.class, DynamicConfigurationChanger.class);

            APPEND_KEY = ConfigurationUtil.class.getDeclaredMethod("appendKey", List.class, Object.class);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Constructor.
     * Please refer to individual fields for comments.
     */
    DirectProxyAsmGenerator(
            ConfigurationAsmGenerator cgen,
            Class<?> schemaClass,
            Set<Class<?>> internalExtensions,
            List<Field> schemaFields,
            Collection<Field> internalExtensionsFields,
            Field internalIdField
    ) {
        this.cgen = cgen;
        this.schemaClass = schemaClass;
        this.internalExtensions = internalExtensions;
        this.schemaFields = schemaFields;
        this.internalExtensionsFields = internalExtensionsFields;
        this.internalIdField = internalIdField;
    }

    /**
     * Generates class definition. Expected to be called once at most.
     */
    public ClassDefinition generate() {
        assert classDef == null;

        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaClass);

        // public final class FooDirectProxy extends DirectConfigurationProxy<Object, Object> implements FooConfiguration, ...
        classDef = new ClassDefinition(
                of(PUBLIC, FINAL),
                internalName(schemaClassInfo.directProxyClassName),
                type(DirectConfigurationProxy.class),
                cgen.configClassInterfaces(schemaClass, internalExtensions)
        );

        addConstructor();

        if (internalIdField != null) {
            addGetMethod(internalIdField);
        }

        for (Field schemaField : concat(schemaFields, internalExtensionsFields)) {
            addGetMethod(schemaField);
        }

        return classDef;
    }

    /**
     * Generates constructor.
     */
    private void addConstructor() {
        // public FooDirectProxy(List<KeyPathNode> keys, DynamicConfigurationChanger changer) {
        MethodDefinition ctor = classDef.declareConstructor(
                of(PUBLIC),
                arg("keys", List.class),
                arg("changer", DynamicConfigurationChanger.class)
        );

        //     super(keys, changer);
        // }
        ctor.getBody()
                .append(ctor.getThis())
                .append(ctor.getScope().getVariable("keys"))
                .append(ctor.getScope().getVariable("changer"))
                .invokeConstructor(DIRECT_CFG_CTOR)
                .ret();
    }

    /**
     * Generates getter based on the field.
     */
    private void addGetMethod(Field schemaField) {
        Class<?> schemaFieldType = schemaField.getType();

        String fieldName = schemaField.getName();

        SchemaClassesInfo schemaClassInfo = cgen.schemaInfo(schemaFieldType);

        ParameterizedType returnType;

        // Return type is determined like in ConfigurationImpl class.
        if (isConfigValue(schemaField)) {
            returnType = typeFromJavaClassName(schemaClassInfo.cfgClassName);
        } else if (isNamedConfigValue(schemaField)) {
            returnType = type(NamedConfigurationTree.class);
        } else {
            assert isValue(schemaField) || isPolymorphicId(schemaField) || isInjectedName(schemaField)
                    || isInternalId(schemaField) : schemaField;

            returnType = type(ConfigurationValue.class);
        }

        MethodDefinition methodDef = classDef.declareMethod(
                of(PUBLIC),
                fieldName,
                returnType
        );

        BytecodeBlock body = methodDef.getBody();

        if (isValue(schemaField) || isPolymorphicId(schemaField) || isInjectedName(schemaField) || isInternalId(schemaField)) {
            // new DirectValueProxy(appendKey(this.keys, new KeyPathNode("name")), changer);
            // or
            // new DirectValueProxy(appendKey(this.keys, new KeyPathNode("<internal_id>")), changer);
            body.append(newInstance(
                    DirectValueProxy.class,
                    invokeStatic(
                            APPEND_KEY,
                            methodDef.getThis().getField("keys", List.class),
                            newInstance(
                                    KeyPathNode.class,
                                    constantString(isInjectedName(schemaField) ? InnerNode.INJECTED_NAME
                                            : isInternalId(schemaField) ? InnerNode.INTERNAL_ID : fieldName)
                            )
                    ),
                    methodDef.getThis().getField("changer", DynamicConfigurationChanger.class)
            ));
        } else {
            SchemaClassesInfo fieldSchemaClassInfo = cgen.schemaInfo(schemaField.getType());

            ParameterizedType resultType = typeFromJavaClassName(fieldSchemaClassInfo.directProxyClassName);

            if (isConfigValue(schemaField)) {
                // new BarDirectProxy(appendKey(this.keys, new KeyPathNode("name")), changer);
                body.append(newInstance(
                        resultType,
                        invokeStatic(
                                APPEND_KEY,
                                methodDef.getThis().getField("keys", List.class),
                                newInstance(KeyPathNode.class, constantString(fieldName))
                        ),
                        methodDef.getThis().getField("changer", DynamicConfigurationChanger.class)
                ));
            } else {
                // new DirectNamedListProxy(appendKey(this.keys, new KeyPathNode("name")), changer, BarDirectProxy::new);
                body.append(newInstance(
                        DirectNamedListProxy.class,
                        invokeStatic(
                                APPEND_KEY,
                                methodDef.getThis().getField("keys", List.class),
                                newInstance(KeyPathNode.class, constantString(fieldName))
                        ),
                        methodDef.getThis().getField("changer", DynamicConfigurationChanger.class),
                        newDirectProxyLambda(fieldSchemaClassInfo)
                ));
            }
        }

        // Return object from the above.
        body.retObject();
    }

    /**
     * Returns expression for {@code BarDirectProxy::new} lambda.
     */
    @NotNull
    public static BytecodeExpression newDirectProxyLambda(SchemaClassesInfo schemaClassInfo) {
        return invokeDynamic(
                LAMBDA_METAFACTORY,
                asList(
                        // Erased method type.
                        getMethodType(getType(Object.class), getType(Object.class), getType(Object.class)),
                        new Handle(
                                H_NEWINVOKESPECIAL,
                                internalName(schemaClassInfo.directProxyClassName),
                                "<init>",
                                // Descriptor of the constructor.
                                getMethodDescriptor(VOID_TYPE, getType(List.class), getType(DynamicConfigurationChanger.class)),
                                false
                        ),
                        // Not-erased method type.
                        getMethodType(
                                typeFromJavaClassName(schemaClassInfo.directProxyClassName).getAsmType(),
                                getType(List.class),
                                getType(DynamicConfigurationChanger.class)
                        )
                ),
                "apply", // java.util.function.BiFunction.apply
                methodType(BiFunction.class)
        );
    }
}
