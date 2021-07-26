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

import java.io.Serializable;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.TypeUtils;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.SYNTHETIC;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromJavaClassName;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.inlineIf;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.isNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.singleton;
import static java.util.EnumSet.of;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

/**
 * This class is responsible for generating internal implementation classes for configuration schemas. It uses classes
 * from {@code bytecode} module to achieve this goal, like {@link ClassGenerator}, for examples.
 */
public class ConfigurationAsmGenerator {
    /** {@link LambdaMetafactory#metafactory(Lookup, String, MethodType, MethodType, MethodHandle, MethodType)} */
    private static final Method LAMBDA_METAFACTORY;

    /** {@link Consumer#accept(Object)}*/
    private static final Method ACCEPT;

    /** {@link ConfigurationVisitor#visitLeafNode(String, Serializable)} */
    private static final Method VISIT_LEAF;

    /** {@link ConfigurationVisitor#visitInnerNode(String, InnerNode)} */
    private static final Method VISIT_INNER;

    /** {@link ConfigurationVisitor#visitNamedListNode(String, NamedListNode)} */
    private static final Method VISIT_NAMED;

    /** {@link ConfigurationSource#unwrap(Class)} */
    private static final Method UNWRAP;

    /** {@link ConfigurationSource#descend(ConstructableTreeNode)} */
    private static final Method DESCEND;

    /** {@link ConstructableTreeNode#copy()} */
    private static final Method COPY;

    /** {@link DynamicConfiguration#DynamicConfiguration(List, String, RootKey, ConfigurationChanger)} */
    private static final Constructor DYNAMIC_CONFIGURATION_CTOR;

    /** {@link DynamicConfiguration#add(ConfigurationProperty)} */
    private static final Method DYNAMIC_CONFIGURATION_ADD;

    static {
        try {
            LAMBDA_METAFACTORY = LambdaMetafactory.class.getDeclaredMethod(
                "metafactory",
                Lookup.class,
                String.class,
                MethodType.class,
                MethodType.class,
                MethodHandle.class,
                MethodType.class
            );

            ACCEPT = Consumer.class.getDeclaredMethod("accept", Object.class);

            VISIT_LEAF = ConfigurationVisitor.class.getDeclaredMethod("visitLeafNode", String.class, Serializable.class);

            VISIT_INNER = ConfigurationVisitor.class.getDeclaredMethod("visitInnerNode", String.class, InnerNode.class);

            VISIT_NAMED = ConfigurationVisitor.class.getDeclaredMethod("visitNamedListNode", String.class, NamedListNode.class);

            UNWRAP = ConfigurationSource.class.getDeclaredMethod("unwrap", Class.class);

            DESCEND = ConfigurationSource.class.getDeclaredMethod("descend", ConstructableTreeNode.class);

            COPY = ConstructableTreeNode.class.getDeclaredMethod("copy");

            DYNAMIC_CONFIGURATION_CTOR = DynamicConfiguration.class.getDeclaredConstructor(
                List.class,
                String.class,
                RootKey.class,
                ConfigurationChanger.class
            );

            DYNAMIC_CONFIGURATION_ADD = DynamicConfiguration.class.getDeclaredMethod(
                "add",
                ConfigurationProperty.class
            );
        }
        catch (NoSuchMethodException nsme) {
            throw new ExceptionInInitializerError(nsme);
        }
    }

    /** Information about schema classes - bunch of names and dynamically compiled internal classes. */
    private final Map<Class<?>, SchemaClassesInfo> schemasInfo = new HashMap<>();

    /** Class generator instance. */
    private final ClassGenerator generator = ClassGenerator.classGenerator(this.getClass().getClassLoader());

    /**
     * Creates new instance of {@code *Node} class corresponding to the given Configuration Schema.
     * @param schemaClass Configuration Schema class.
     * @return Node instance.
     */
    public synchronized InnerNode instantiateNode(Class<?> schemaClass) {
        SchemaClassesInfo info = schemasInfo.get(schemaClass);

        assert info != null && info.nodeClass != null : schemaClass;

        try {
            Constructor<? extends InnerNode> constructor = info.nodeClass.getConstructor();

            assert constructor.canAccess(null);

            return constructor.newInstance();
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates new instance of {@code *Configuration} class corresponding to the given Configuration Schema.
     * @param rootKey Root key of the configuration root.
     * @param changer Configuration changer instance to pass into constructor.
     * @return Configuration instance.
     */
    public synchronized DynamicConfiguration<?, ?> instantiateCfg(
        RootKey<?, ?> rootKey,
        ConfigurationChanger changer
    ) {
        SchemaClassesInfo info = schemasInfo.get(rootKey.schemaClass());

        assert info != null && info.cfgImplClass != null;

        try {
            Constructor<? extends DynamicConfiguration<?, ?>> constructor = info.cfgImplClass.getConstructor(
                List.class,
                String.class,
                RootKey.class,
                ConfigurationChanger.class
            );

            assert constructor.canAccess(null);

            return constructor.newInstance(Collections.emptyList(), rootKey.key(), rootKey, changer);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Generates, defines, loads and initializes all dynamic classes required for the given Configuration Schema.
     * @param rootSchemaClass Class of the root Configuration Schema.
     */
    public synchronized void compileRootSchema(Class<?> rootSchemaClass) {
        if (schemasInfo.containsKey(rootSchemaClass))
            return; // Already compiled.

        Queue<Class<?>> compileQueue = new LinkedList<>();
        compileQueue.add(rootSchemaClass);

        schemasInfo.put(rootSchemaClass, new SchemaClassesInfo(rootSchemaClass));

        Set<Class<?>> schemas = new HashSet<>();
        List<ClassDefinition> definitions = new ArrayList<>();

        while (!compileQueue.isEmpty()) {
            Class<?> schemaClass = compileQueue.poll();

            assert schemaClass.isAnnotationPresent(ConfigurationRoot.class)
                || schemaClass.isAnnotationPresent(Config.class)
                : schemaClass + " is not properly annotated";

            assert schemasInfo.containsKey(schemaClass);

            for (Field field : schemaClass.getDeclaredFields()) {
                if (isConfigValue(field) || isNamedConfigValue(field)) {
                    Class<?> subSchemaClass = field.getType();

                    if (!schemasInfo.containsKey(subSchemaClass)) {
                        compileQueue.offer(subSchemaClass);
                        schemasInfo.put(subSchemaClass, new SchemaClassesInfo(subSchemaClass));
                    }
                }
            }

            schemas.add(schemaClass);
            definitions.add(createNodeClass(schemaClass));
            definitions.add(createCfgImplClass(schemaClass));
        }

        Map<String, Class<?>> definedClasses = generator.defineClasses(definitions);

        for (Class<?> schemaClass : schemas) {
            SchemaClassesInfo info = schemasInfo.get(schemaClass);

            info.nodeClass = (Class<? extends InnerNode>)definedClasses.get(info.nodeClassName);
            info.cfgImplClass = (Class<? extends DynamicConfiguration<?, ?>>)definedClasses.get(info.cfgImplClassName);
        }
    }

    private ClassDefinition createNodeClass(Class<?> schemaClass) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        // Node class definition.
        ClassDefinition classDef = new ClassDefinition(
            of(PUBLIC, FINAL),
            internalName(schemaClassInfo.nodeClassName),
            type(InnerNode.class),
            typeFromJavaClassName(schemaClassInfo.viewClassName),
            typeFromJavaClassName(schemaClassInfo.changeClassName)
        );

        // Spec field.
        FieldDefinition specField = classDef.declareField(of(PRIVATE, FINAL), "_spec", schemaClass);

        // org.apache.ignite.internal.configuration.tree.InnerNode#schemaType
        addNodeSchemaTypeMethod(classDef, specField);

        // Cached array of reflected fields. Helps to avoid unnecessary clonings.
        Field[] schemaFields = schemaClass.getDeclaredFields();

        // Define the rest of the fields.
        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        for (Field schemaField : schemaFields) {
            assert isValue(schemaField) || isConfigValue(schemaField) || isNamedConfigValue(schemaField) : schemaField;

            fieldDefs.put(schemaField.getName(), addNodeField(classDef, schemaField));
        }

        // Constructor.
        addNodeConstructor(classDef, schemaClass, specField, schemaFields, fieldDefs);

        // VIEW and CHANGE methods.
        for (Field schemaField : schemaFields) {
            FieldDefinition fieldDef = fieldDefs.get(schemaField.getName());

            addNodeViewMethod(classDef, schemaField, fieldDef);

            addNodeChangeMethod(classDef, schemaField, fieldDef, schemaClassInfo);
        }

        // traverseChildren
        addNodeTraverseChildrenMethod(classDef, schemaFields, fieldDefs);

        // traverseChild
        addNodeTraverseChildMethod(classDef, schemaFields, fieldDefs);

        // construct
        addNodeConstructMethod(classDef, schemaFields, fieldDefs);

        // constructDefault
        addNodeConstructDefaultMethod(classDef, specField, schemaFields, fieldDefs);

        return classDef;
    }

    /**
     * Checks whether configuration schema field represents primitive configuration value.
     * @param schemaField Configuration Schema class field.
     * @return {@code true} if field represents primitive configuration.
     */
    private static boolean isValue(Field schemaField) {
        return schemaField.isAnnotationPresent(Value.class);
    }

    /**
     * Checks whether configuration schema field represents regular configuration value.
     * @param schemaField Configuration Schema class field.
     * @return {@code true} if field represents regular configuration.
     */
    private static boolean isConfigValue(Field schemaField) {
        return schemaField.isAnnotationPresent(ConfigValue.class);
    }

    /**
     * Checks whether configuration schema field represents named list configuration value.
     * @param schemaField Configuration Schema class field.
     * @return {@code true} if field represents named list configuration.
     */
    private static boolean isNamedConfigValue(Field schemaField) {
        return schemaField.isAnnotationPresent(NamedConfigValue.class);
    }

    /**
     * Add {@link InnerNode#schemaType()} method implementation to the class. It looks like the following code:
     * <pre><code>
     * public Class schemaType() {
     *     return this._spec.getClass();
     * }
     * </code></pre>
     * @param classDef Class definition.
     * @param specField Field definition of the {@code _spec} field.
     */
    private void addNodeSchemaTypeMethod(ClassDefinition classDef, FieldDefinition specField) {
        MethodDefinition schemaTypeMtd = classDef.declareMethod(of(PUBLIC), "schemaType", type(Class.class));

        schemaTypeMtd.getBody().append(
            schemaTypeMtd.getThis().getField(specField).invoke("getClass", Class.class)
        ).retObject();
    }

    /**
     * Declares field that corresponds to configuration value. Depending on the schema, 3 options possible:
     * <ul>
     *     <li>
     *         {@code @Value public type fieldName}<br/>becomes<br/>
     *         {@code private BoxedType fieldName}
     *     </li>
     *     <li>
     *         {@code @ConfigValue public MyConfigurationSchema fieldName}<br/>becomes<br/>
     *         {@code private MyNode fieldName}
     *     </li>
     *     <li>
     *         {@code @NamedConfigValue public type fieldName}<br/>becomes<br/>
     *         {@code private NamedListNode fieldName}
     *     </li>
     * </ul>
     * @param classDef Node class definition.
     * @param schemaField Configuration Schema class field.
     * @return Declared field definition.
     */
    private FieldDefinition addNodeField(ClassDefinition classDef, Field schemaField) {
        Class<?> schemaFieldClass = schemaField.getType();

        ParameterizedType nodeFieldType;

        if (isValue(schemaField))
            nodeFieldType = type(box(schemaFieldClass));
        else if (isConfigValue(schemaField))
            nodeFieldType = typeFromJavaClassName(schemasInfo.get(schemaFieldClass).nodeClassName);
        else
            nodeFieldType = type(NamedListNode.class);

        return classDef.declareField(of(PRIVATE), schemaField.getName(), nodeFieldType);
    }

    /**
     * Implements default constructor for the node class. It initializes {@code _spec} field and every other field
     * that represents named list configuration.
     * @param classDef Node class definition.
     * @param schemaClass Configuration Schema class.
     * @param specField Field definition for the {@code _spec} field of the node class.
     * @param schemaFields Configuration Schema class fields.
     * @param fieldDefs Field definitions for all fields of node class excluding {@code _spec}.
     */
    private void addNodeConstructor(
        ClassDefinition classDef,
        Class<?> schemaClass,
        FieldDefinition specField,
        Field[] schemaFields,
        Map<String, FieldDefinition> fieldDefs
    ) {
        MethodDefinition ctor = classDef.declareConstructor(of(PUBLIC));

        // super();
        ctor.getBody().append(ctor.getThis()).invokeConstructor(InnerNode.class);

        // this._spec = new MyConfigurationSchema();
        ctor.getBody().append(ctor.getThis().setField(specField, newInstance(schemaClass)));

        for (Field schemaField : schemaFields) {
            if (!isNamedConfigValue(schemaField))
                continue;

            NamedConfigValue namedCfgAnnotation = schemaField.getAnnotation(NamedConfigValue.class);

            SchemaClassesInfo fieldClassNames = new SchemaClassesInfo(schemaField.getType());

            // this.values = new NamedListNode<>(key, ValueNode::new);
            ctor.getBody().append(ctor.getThis().setField(
                fieldDefs.get(schemaField.getName()),
                newInstance(
                    NamedListNode.class,
                    constantString(namedCfgAnnotation.syntheticKeyName()),
                    newNamedListElementLambda(fieldClassNames.nodeClassName)
                )
            ));
        }

        // return;
        ctor.getBody().ret();
    }

    /**
     * Implements getter method from {@code VIEW} interface. It returns field value, possibly unboxed or cloned,
     * depending on type.
     * @param classDef Node class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldDefs Field definition.
     */
    private void addNodeViewMethod(
        ClassDefinition classDef,
        Field schemaField,
        FieldDefinition fieldDef
    ) {
        Class<?> schemaFieldType = schemaField.getType();

        ParameterizedType returnType;

        // Return type is either corresponding VIEW type or the same type as declared in schema.
        if (isConfigValue(schemaField))
            returnType = typeFromJavaClassName(schemasInfo.get(schemaFieldType).viewClassName);
        else if (isNamedConfigValue(schemaField))
            returnType = type(NamedListView.class);
        else
            returnType = type(schemaFieldType);

        String fieldName = schemaField.getName();

        MethodDefinition viewMtd = classDef.declareMethod(
            of(PUBLIC),
            fieldName,
            returnType
        );

        // result = this.field;
        viewMtd.getBody().append(viewMtd.getThis().getField(fieldDef));

        // result = Box.boxValue(result); // Unboxing.
        if (schemaFieldType.isPrimitive()) {
            viewMtd.getBody().invokeVirtual(
                box(schemaFieldType),
                schemaFieldType.getSimpleName() + "Value",
                schemaFieldType
            );
        }

        // retuls = result.clone();
        if (schemaFieldType.isArray())
            viewMtd.getBody().invokeVirtual(schemaFieldType, "clone", Object.class).checkCast(schemaFieldType);

        // return result;
        viewMtd.getBody().ret(schemaFieldType);
    }

    /**
     * Implements changer method from {@code CHANGE} interface.
     * @param classDef Node class definition.
     * @param schemaField Configuration Schema class field.
     * @param fieldDef Field definition.
     * @param schemaClassInfo {@link SchemaClassesInfo} for Configuration Schema that contains original field.
     */
    private void addNodeChangeMethod(
        ClassDefinition classDef,
        Field schemaField,
        FieldDefinition fieldDef,
        SchemaClassesInfo schemaClassInfo
    ) {
        Class<?> schemaFieldType = schemaField.getType();

        MethodDefinition changeMtd = classDef.declareMethod(
            of(PUBLIC),
            "change" + capitalize(schemaField.getName()),
            typeFromJavaClassName(schemaClassInfo.changeClassName),
            // Change argument type is a Consumer for all inner or named fields.
            arg("change", isValue(schemaField) ? type(schemaFieldType) : type(Consumer.class))
        );

        BytecodeBlock changeBody = changeMtd.getBody();

        if (isValue(schemaField)) {
            // newValue = change;
            BytecodeExpression newValue = changeMtd.getScope().getVariable("change");

            // newValue = Box.valueOf(newValue); // Boxing.
            if (schemaFieldType.isPrimitive())
                newValue = invokeStatic(fieldDef.getType(), "valueOf", fieldDef.getType(), singleton(newValue));

            // newValue = newValue.clone();
            if (schemaFieldType.isArray())
                newValue = newValue.invoke("clone", Object.class).cast(schemaFieldType);

            // this.field = newValue;
            changeBody.append(changeMtd.getThis().setField(fieldDef, newValue));
        }
        else {
            // this.field = (this.field == null) ? new ValueNode() : (ValueNode)this.field.copy();
            changeBody.append(copyNodeField(changeMtd, fieldDef));

            // change.accept(this.field);
            changeBody.append(changeMtd.getScope().getVariable("change").invoke(
                ACCEPT,
                changeMtd.getThis().getField(fieldDef)
            ));
        }

        // return this;
        changeBody.append(changeMtd.getThis()).retObject();
    }

    /**
     * Implements {@link InnerNode#traverseChildren(ConfigurationVisitor)} method.
     * @param classDef Class definition.
     * @param schemaFields Array of all schema fields that represent configurations.
     * @param fieldDefs Definitions for all fields in {@code schemaFields}.
     */
    private void addNodeTraverseChildrenMethod(
        ClassDefinition classDef,
        Field[] schemaFields,
        Map<String, FieldDefinition> fieldDefs
    ) {
        MethodDefinition traverseChildrenMtd = classDef.declareMethod(
            of(PUBLIC),
            "traverseChildren",
            type(void.class),
            arg("visitor", type(ConfigurationVisitor.class))
        ).addException(NoSuchElementException.class);

        BytecodeBlock mtdBody = traverseChildrenMtd.getBody();

        for (Field schemaField : schemaFields) {
            FieldDefinition fieldDef = fieldDefs.get(schemaField.getName());

            // Visit every field. Returned value isn't used so we pop it off the stack.
            mtdBody.append(invokeVisit(traverseChildrenMtd, schemaField, fieldDef).pop());
        }

        mtdBody.ret();
    }

    /**
     * Implements {@link InnerNode#traverseChild(String, ConfigurationVisitor)} method.
     * @param classDef Class definition.
     * @param schemaFields Array of all schema fields that represent configurations.
     * @param fieldDefs Definitions for all fields in {@code schemaFields}.
     */
    private void addNodeTraverseChildMethod(
        ClassDefinition classDef,
        Field[] schemaFields,
        Map<String, FieldDefinition> fieldDefs
    ) {
        MethodDefinition traverseChildMtd = classDef.declareMethod(
            of(PUBLIC),
            "traverseChild",
            type(Object.class),
            arg("key", type(String.class)),
            arg("visitor", type(ConfigurationVisitor.class))
        ).addException(NoSuchElementException.class);

        Variable keyVar = traverseChildMtd.getScope().getVariable("key");

        // Here we have a switch by passed kay parameter.
        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(traverseChildMtd.getScope())
            .expression(keyVar);

        for (Field schemaField : schemaFields) {
            String fieldName = schemaField.getName();

            FieldDefinition fieldDef = fieldDefs.get(fieldName);

            // Visit result should be immediately returned.
            switchBuilder.addCase(fieldName, invokeVisit(traverseChildMtd, schemaField, fieldDef).retObject());
        }

        // Default option is to throw "NoSuchElementException(key)".
        switchBuilder.defaultCase(new BytecodeBlock()
            .append(newInstance(NoSuchElementException.class, keyVar))
            .throwObject()
        );

        // No default "return" statement required.
        traverseChildMtd.getBody().append(switchBuilder.build());
    }

    /**
     * Creates bytecode block that invokes one of {@link ConfigurationVisitor}'s methods.
     * @param mtd Method definition, either {@link InnerNode#traverseChildren(ConfigurationVisitor)} or
     *      {@link InnerNode#traverseChild(String, ConfigurationVisitor)} defined in {@code *Node} class.
     * @param schemaField Configuration Schema field to visit.
     * @param fieldDef Field definition from current class.
     * @return Bytecode block that invokes "visit*" method.
     */
    private BytecodeBlock invokeVisit(MethodDefinition mtd, Field schemaField, FieldDefinition fieldDef) {
        Method visitMethod;

        if (isValue(schemaField))
            visitMethod = VISIT_LEAF;
        else if (isConfigValue(schemaField))
            visitMethod = VISIT_INNER;
        else
            visitMethod = VISIT_NAMED;

        return new BytecodeBlock().append(mtd.getScope().getVariable("visitor").invoke(
            visitMethod,
            constantString(fieldDef.getName()),
            mtd.getThis().getField(fieldDef)
        ));
    }

    /**
     * Implements {@link ConstructableTreeNode#construct(String, ConfigurationSource)} method.
     * @param classDef Class definition.
     * @param schemaFields Array of all schema fields that represent configurations.
     * @param fieldDefs Definitions for all fields in {@code schemaFields}.
     */
    private void addNodeConstructMethod(
        ClassDefinition classDef,
        Field[] schemaFields,
        Map<String, FieldDefinition> fieldDefs
    ) {
        MethodDefinition constructMtd = classDef.declareMethod(
            of(PUBLIC),
            "construct",
            type(void.class),
            arg("key", String.class),
            arg("src", ConfigurationSource.class)
        ).addException(NoSuchElementException.class);

        Variable keyVar = constructMtd.getScope().getVariable("key");
        Variable srcVar = constructMtd.getScope().getVariable("src");

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(constructMtd.getScope())
            .expression(keyVar);

        for (Field schemaField : schemaFields) {
            FieldDefinition fieldDef = fieldDefs.get(schemaField.getName());

            BytecodeBlock caseClause = new BytecodeBlock();

            switchBuilder.addCase(schemaField.getName(), caseClause);

            // this.field = src == null ? null : src.unwrap(FieldType.class);
            if (isValue(schemaField)) {
                caseClause.append(constructMtd.getThis().setField(fieldDef, inlineIf(
                    isNull(srcVar),
                    constantNull(fieldDef.getType()),
                    srcVar.invoke(UNWRAP, constantClass(fieldDef.getType())).cast(fieldDef.getType())
                )));
            }
            // this.field = src == null ? null : src.descend(field = (field == null ? new FieldType() : field.copy()));
            else if (isConfigValue(schemaField)) {
                caseClause.append(new IfStatement()
                    .condition(isNull(srcVar))
                    .ifTrue(constructMtd.getThis().setField(fieldDef, constantNull(fieldDef.getType())))
                    .ifFalse(new BytecodeBlock()
                        .append(copyNodeField(constructMtd, fieldDef))
                        .append(srcVar.invoke(DESCEND, constructMtd.getThis().getField(fieldDef)))
                    )
                );
            }
            // this.field = src == null ? new NamedListNode<>(key, ValueNode::new) : src.descend(field = field.copy()));
            else {
                NamedConfigValue namedCfgAnnotation = schemaField.getAnnotation(NamedConfigValue.class);

                String fieldNodeClassName = schemasInfo.get(schemaField.getType()).nodeClassName;

                caseClause.append(new IfStatement()
                    .condition(isNull(srcVar))
                    .ifTrue(constructMtd.getThis().setField(
                        fieldDef,
                        newInstance(
                            NamedListNode.class,
                            constantString(namedCfgAnnotation.syntheticKeyName()),
                            newNamedListElementLambda(fieldNodeClassName)
                        )
                    ))
                    .ifFalse(new BytecodeBlock()
                        .append(constructMtd.getThis().setField(
                            fieldDef,
                            constructMtd.getThis().getField(fieldDef).invoke(COPY).cast(fieldDef.getType())
                        ))
                        .append(srcVar.invoke(DESCEND, constructMtd.getThis().getField(fieldDef)))
                    )
                );
            }
        }

        // Default option is to throw "NoSuchElementException(key)".
        switchBuilder.defaultCase(new BytecodeBlock()
            .append(newInstance(NoSuchElementException.class, keyVar))
            .throwObject()
        );

        constructMtd.getBody().append(switchBuilder.build()).ret();
    }

    /**
     * Implements {@link InnerNode#constructDefault(String)} method.
     * @param classDef Node class definition.
     * @param specField Field definition for the {@code _spec} field of the node class.
     * @param schemaFields Configuration Schema class fields.
     * @param fieldDefs Field definitions for all fields of node class excluding {@code _spec}.
     */
    private void addNodeConstructDefaultMethod(
        ClassDefinition classDef,
        FieldDefinition specField,
        Field[] schemaFields,
        Map<String, FieldDefinition> fieldDefs
    ) {
        MethodDefinition constructDfltMtd = classDef.declareMethod(
            of(PUBLIC),
            "constructDefault",
            type(void.class),
            arg("key", String.class)
        ).addException(NoSuchElementException.class);

        Variable keyVar = constructDfltMtd.getScope().getVariable("key");

        StringSwitchBuilder switchBuilder = new StringSwitchBuilder(constructDfltMtd.getScope())
            .expression(keyVar);

        for (Field schemaField : schemaFields) {
            if (!isValue(schemaField))
                continue;

            if (!schemaField.getAnnotation(Value.class).hasDefault()) {
                switchBuilder.addCase(schemaField.getName(), new BytecodeBlock());

                continue;
            }

            FieldDefinition fieldDef = fieldDefs.get(schemaField.getName());

            Class<?> schemaFieldType = schemaField.getType();

            // defaultValue = _spec.field;
            BytecodeExpression defaultValue = constructDfltMtd.getThis().getField(specField).getField(schemaField);

            // defaultValue = Box.valueOf(defaultValue); // Boxing.
            if (schemaFieldType.isPrimitive()) {
                defaultValue = invokeStatic(
                    fieldDef.getType(),
                    "valueOf",
                    fieldDef.getType(),
                    singleton(defaultValue)
                );
            }

            // defaultValue = defaultValue.clone();
            if (schemaFieldType.isArray())
                defaultValue = defaultValue.invoke("clone", Object.class).cast(schemaFieldType);

            // this.field = defaultValue;
            BytecodeBlock caseClause = new BytecodeBlock()
                .append(constructDfltMtd.getThis().setField(fieldDef, defaultValue));

            switchBuilder.addCase(schemaField.getName(), caseClause);
        }

        // Default option is to throw "NoSuchElementException(key)".
        switchBuilder.defaultCase(new BytecodeBlock()
            .append(newInstance(NoSuchElementException.class, keyVar))
            .throwObject()
        );

        constructDfltMtd.getBody().append(switchBuilder.build()).ret();
    }

    /**
     * Copies field into itself or instantiates it if the field is null.
     * @param mtd Method definition.
     * @param fieldDef Field definition.
     * @return Bytecode expression.
     */
    @NotNull private BytecodeExpression copyNodeField(MethodDefinition mtd, FieldDefinition fieldDef) {
        return mtd.getThis().setField(fieldDef, inlineIf(
            isNull(mtd.getThis().getField(fieldDef)),
            newInstance(fieldDef.getType()),
            mtd.getThis().getField(fieldDef).invoke(COPY).cast(fieldDef.getType())
        ));
    }

    /**
     * Creates {@code *Node::new} lambda expression with {@link Supplier} type.
     * @param nodeClassName Name of the {@code *Node} class.
     * @return InvokeDynamic bytecode expression.
     */
    @NotNull private BytecodeExpression newNamedListElementLambda(String nodeClassName) {
        return invokeDynamic(
            LAMBDA_METAFACTORY,
            Arrays.asList(
                getMethodType(getType(Object.class)),
                new Handle(
                    H_NEWINVOKESPECIAL,
                    internalName(nodeClassName),
                    "<init>",
                    getMethodDescriptor(Type.VOID_TYPE),
                    false
                ),
                getMethodType(typeFromJavaClassName(nodeClassName).getAsmType())
            ),
            "get",
            methodType(Supplier.class)
        );
    }

    private ClassDefinition createCfgImplClass(Class<?> schemaClass) {
        SchemaClassesInfo schemaClassInfo = schemasInfo.get(schemaClass);

        // Configuration impl class definition.
        ClassDefinition classDef = new ClassDefinition(
            of(PUBLIC, FINAL),
            internalName(schemaClassInfo.cfgImplClassName),
            type(DynamicConfiguration.class),
            typeFromJavaClassName(schemaClassInfo.cfgClassName)
        );

        // Cached array of reflected fields. Helps to avoid unnecessary clonings.
        Field[] schemaFields = schemaClass.getDeclaredFields();

        // Fields.
        Map<String, FieldDefinition> fieldDefs = new HashMap<>();

        for (Field schemaField : schemaFields)
            fieldDefs.put(schemaField.getName(), addConfigurationImplField(classDef, schemaField));

        // Constructor
        addConfigurationImplConstructor(classDef, schemaClassInfo, schemaFields, fieldDefs);

        for (Field schemaField : schemaFields) {
            addConfigurationImplGetMethod(classDef, schemaClass, fieldDefs, schemaField);
        }

        return classDef;
    }

    /**
     * Declares field that corresponds to configuration value. Depending on the schema, 3 options possible:
     * <ul>
     *     <li>
     *         {@code @Value public type fieldName}<br/>becomes<br/>
     *         {@code private DynamicProperty fieldName}
     *     </li>
     *     <li>
     *         {@code @ConfigValue public MyConfigurationSchema fieldName}<br/>becomes<br/>
     *         {@code private MyConfiguration fieldName}
     *     </li>
     *     <li>
     *         {@code @NamedConfigValue public type fieldName}<br/>becomes<br/>
     *         {@code private NamedListConfiguration fieldName}
     *     </li>
     * </ul>
     * @param classDef Configuration impl class definition.
     * @param schemaField Configuration Schema class field.
     * @return Declared field definition.
     */
    private FieldDefinition addConfigurationImplField(ClassDefinition classDef, Field schemaField) {
        ParameterizedType fieldType;

        if (isConfigValue(schemaField))
            fieldType = typeFromJavaClassName(schemasInfo.get(schemaField.getType()).cfgClassName);
        else if (isNamedConfigValue(schemaField))
            fieldType = type(NamedListConfiguration.class);
        else
            fieldType = type(DynamicProperty.class);

        return classDef.declareField(of(PRIVATE), schemaField.getName(), fieldType);
    }

    /**
     * Implements default constructor for the configuration class. It initializes all fields and adds them to members
     * collection.
     * @param classDef Configuration impl class definition.
     * @param schemaClassInfo Configuration Schema class info.
     * @param schemaFields Configuration Schema class fields.
     * @param fieldDefs Field definitions for all fields of configuration impl class.
     */
    private void addConfigurationImplConstructor(
        ClassDefinition classDef,
        SchemaClassesInfo schemaClassInfo,
        Field[] schemaFields,
        Map<String, FieldDefinition> fieldDefs
    ) {
        MethodDefinition ctor = classDef.declareConstructor(
            of(PUBLIC),
            arg("prefix", List.class),
            arg("key", String.class),
            arg("rootKey", RootKey.class),
            arg("changer", ConfigurationChanger.class)
        );

        BytecodeBlock ctorBody = ctor.getBody()
            .append(ctor.getThis())
            .append(ctor.getScope().getVariable("prefix"))
            .append(ctor.getScope().getVariable("key"))
            .append(ctor.getScope().getVariable("rootKey"))
            .append(ctor.getScope().getVariable("changer"))
            .invokeConstructor(DYNAMIC_CONFIGURATION_CTOR);

        int newIdx = 0;
        for (Field schemaField : schemaFields) {
            FieldDefinition fieldDef = fieldDefs.get(schemaField.getName());

            BytecodeExpression newValue;

            if (isValue(schemaField)) {
                // newValue = new DynamicProperty(super.keys, fieldName, rootKey, changer);
                newValue = newInstance(
                    DynamicProperty.class,
                    ctor.getThis().getField("keys", List.class),
                    constantString(schemaField.getName()),
                    ctor.getScope().getVariable("rootKey"),
                    ctor.getScope().getVariable("changer")
                );
            }
            else {
                SchemaClassesInfo fieldInfo = schemasInfo.get(schemaField.getType());

                ParameterizedType cfgImplParameterizedType = typeFromJavaClassName(fieldInfo.cfgImplClassName);

                if (isConfigValue(schemaField)) {
                    // newValue = new MyConfigurationImpl(super.keys, fieldName, rootKey, changer);
                    newValue = newInstance(
                        cfgImplParameterizedType,
                        ctor.getThis().getField("keys", List.class),
                        constantString(schemaField.getName()),
                        ctor.getScope().getVariable("rootKey"),
                        ctor.getScope().getVariable("changer")
                    );
                }
                else {
                    // We have to create method "$new$<idx>" to reference it in lambda expression. That's the way it
                    // works, it'll invoke constructor with all 4 arguments, not just 2 as in BiFunction.
                    MethodDefinition newMtd = classDef.declareMethod(
                        of(PRIVATE, STATIC, SYNTHETIC),
                        "$new$" + newIdx++,
                        typeFromJavaClassName(fieldInfo.cfgClassName),
                        arg("rootKey", RootKey.class),
                        arg("changer", ConfigurationChanger.class),
                        arg("prefix", List.class),
                        arg("key", String.class)
                    );

                    // newValue = new NamedListConfiguration(super.keys, fieldName, rootKey, changer, (p, k) ->
                    //     new ValueConfigurationImpl(p, k, rootKey, changer)
                    // );
                    newValue = newInstance(
                        NamedListConfiguration.class,
                        ctor.getThis().getField("keys", List.class),
                        constantString(schemaField.getName()),
                        ctor.getScope().getVariable("rootKey"),
                        ctor.getScope().getVariable("changer"),
                        invokeDynamic(
                            LAMBDA_METAFACTORY,
                            Arrays.asList(
                                getMethodType(getType(Object.class), getType(Object.class), getType(Object.class)),
                                new Handle(
                                    Opcodes.H_INVOKESTATIC,
                                    internalName(schemaClassInfo.cfgImplClassName),
                                    newMtd.getName(),
                                    newMtd.getMethodDescriptor(),
                                    false
                                ),
                                getMethodType(
                                    typeFromJavaClassName(fieldInfo.cfgClassName).getAsmType(),
                                    getType(List.class),
                                    getType(String.class)
                                )
                            ),
                            "apply",
                            BiFunction.class,
                            ctor.getScope().getVariable("rootKey"),
                            ctor.getScope().getVariable("changer")
                        )
                    );

                    newMtd.getBody()
                        .append(newInstance(
                            cfgImplParameterizedType,
                            newMtd.getScope().getVariable("prefix"),
                            newMtd.getScope().getVariable("key"),
                            newMtd.getScope().getVariable("rootKey"),
                            newMtd.getScope().getVariable("changer")
                        ))
                        .retObject();
                }
            }

            // this.field = newValue;
            ctorBody.append(ctor.getThis().setField(fieldDef, newValue));

            // add(this.field);
            ctorBody.append(ctor.getThis().invoke(DYNAMIC_CONFIGURATION_ADD, ctor.getThis().getField(fieldDef)));
        }

        ctorBody.ret();
    }

    /**
     * Implements accessor method in configuration impl class.
     * @param classDef Configuration impl class definition.
     * @param schemaClass Configuration Schema class.
     * @param fieldDefs Field definitions for all fields of configuration impl class.
     * @param schemaField Configuration Schema class field.
     */
    private void addConfigurationImplGetMethod(
        ClassDefinition classDef,
        Class<?> schemaClass,
        Map<String, FieldDefinition> fieldDefs,
        Field schemaField
    ) {
        Class<?> schemaFieldType = schemaField.getType();

        String fieldName = schemaField.getName();
        FieldDefinition fieldDef = fieldDefs.get(fieldName);

        ParameterizedType returnType;

        if (isConfigValue(schemaField))
            returnType = typeFromJavaClassName(new SchemaClassesInfo(schemaFieldType).cfgClassName);
        else if (isNamedConfigValue(schemaField))
            returnType = type(NamedConfigurationTree.class);
        else {
            assert isValue(schemaField) : schemaClass;

            returnType = type(ConfigurationValue.class);
        }

        MethodDefinition viewMtd = classDef.declareMethod(
            of(PUBLIC),
            fieldName,
            returnType
        );

        BytecodeBlock viewBody = viewMtd.getBody();

        viewBody
            .append(viewMtd.getThis())
            .getField(fieldDef)
            .retObject();
    }

    /**
     * Replaces first letter in string with its upper-cased variant.
     * @param name Some string.
     * @return Capitalized version of passed string.
     */
    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Returns internalized version of class name, replacing dots with slashes.
     * @param className Class name (with package).
     * @return Internal class name.
     * @see Type#getInternalName(java.lang.Class)
     */
    @NotNull private static String internalName(String className) {
        return className.replace('.', '/');
    }

    /**
     * Creates boxed version of the class. Types that it can box: {@code boolean}, {@code int}, {@code long} and
     *      {@code double}. Other primitive types are not supported by configuration framework.
     * @param clazz Maybe primitive class.
     * @return Not primitive class that represents parameter class.
     */
    private static Class<?> box(Class<?> clazz) {
        Class<?> boxed = TypeUtils.boxed(clazz);

        return boxed == null ? clazz : boxed;
    }
}
