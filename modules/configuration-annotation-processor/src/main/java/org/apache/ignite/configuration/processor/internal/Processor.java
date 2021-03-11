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

package org.apache.ignite.configuration.processor.internal;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.MirroredTypesException;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.DynamicProperty;
import org.apache.ignite.configuration.internal.NamedListConfiguration;
import org.apache.ignite.configuration.processor.internal.validation.ValidationGenerator;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListChange;
import org.apache.ignite.configuration.tree.NamedListInit;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.NamedListView;

import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static org.apache.ignite.configuration.processor.internal.Utils.suppressWarningsUnchecked;

/**
 * Annotation processor that produces configuration classes.
 */
public class Processor extends AbstractProcessor {
    /** Java file padding. */
    private static final String INDENT = "    ";

    /** Wildcard (?) TypeName. */
    private static final TypeName WILDCARD = WildcardTypeName.subtypeOf(Object.class);

    /** Inherit doc javadoc. */
    private static final String INHERIT_DOC = "{@inheritDoc}";

    /** Class file writer. */
    private Filer filer;

    /**
     * Constructor.
     */
    public Processor() {
    }

    /** {@inheritDoc} */
    @Override public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        filer = processingEnv.getFiler();
    }

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        final Elements elementUtils = processingEnv.getElementUtils();

        Map<TypeName, ConfigurationDescription> props = new HashMap<>();

        List<ConfigurationDescription> roots = new ArrayList<>();

        // All classes annotated with @Config
        final List<TypeElement> annotatedConfigs = roundEnvironment
            .getElementsAnnotatedWithAny(Set.of(ConfigurationRoot.class, Config.class)).stream()
            .filter(element -> element.getKind() == ElementKind.CLASS)
            .map(TypeElement.class::cast)
            .collect(Collectors.toList());

        if (annotatedConfigs.isEmpty())
            return false;

        for (TypeElement clazz : annotatedConfigs) {
            // Get package name of the schema class
            final PackageElement elementPackage = elementUtils.getPackageOf(clazz);
            final String packageName = elementPackage.getQualifiedName().toString();

            // Find all the fields of the schema
            final List<VariableElement> fields = clazz.getEnclosedElements().stream()
                .filter(el -> el.getKind() == ElementKind.FIELD)
                .map(VariableElement.class::cast)
                .collect(Collectors.toList());

            ConfigurationRoot rootAnnotation = clazz.getAnnotation(ConfigurationRoot.class);

            // Is root of the configuration
            final boolean isRoot = rootAnnotation != null;
            // Configuration name
            final String configName = isRoot ? rootAnnotation.rootName() : null;

            final ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());

            // Get name for generated configuration class and it's interface
            final ClassName configClass = Utils.getConfigurationName(schemaClassName);
            final ClassName configInterface = Utils.getConfigurationInterfaceName(schemaClassName);

            ConfigurationDescription configDesc = new ConfigurationDescription(
                configClass,
                configName,
                Utils.getViewName(schemaClassName),
                Utils.getInitName(schemaClassName),
                Utils.getChangeName(schemaClassName)
            );

            // If root, then use it's package as package for Selectors and Keys
            if (isRoot)
                roots.add(configDesc);

            TypeSpec.Builder configurationClassBuilder = TypeSpec.classBuilder(configClass)
                .addSuperinterface(configInterface)
                .addModifiers(PUBLIC, FINAL);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                .addModifiers(PUBLIC);

            CodeBlock.Builder constructorBodyBuilder = CodeBlock.builder();

            for (VariableElement field : fields) {
                assert field.getModifiers().contains(PUBLIC) : clazz.getQualifiedName() + "#" + field.getSimpleName();

                Element fieldTypeElement = processingEnv.getTypeUtils().asElement(field.asType());

                // Get original field type (must be another configuration schema or "primitive" like String or long)
                final TypeName baseType = TypeName.get(field.asType());

                final String fieldName = field.getSimpleName().toString();

                // Get configuration types (VIEW, INIT, CHANGE and so on)
                final ConfigurationFieldTypes types = getTypes(field);

                TypeName fieldType = types.getFieldType();
                TypeName viewClassType = types.getViewClassType();
                TypeName initClassType = types.getInitClassType();
                TypeName changeClassType = types.getChangeClassType();

                final ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
                if (confAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @ConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }

                    // Create DynamicConfiguration (descendant) field
                    final FieldSpec nestedConfigField =
                        FieldSpec
                            .builder(fieldType, fieldName, Modifier.PRIVATE, FINAL)
                            .build();

                    configurationClassBuilder.addField(nestedConfigField);

                    // Constructor statement
                    constructorBodyBuilder.addStatement("add($L = new $T(keys, $S, rootKey, changer))", fieldName, fieldType, fieldName);
                }

                final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
                if (namedConfigAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @NamedConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }

                    // Create NamedListConfiguration<> field
                    final FieldSpec nestedConfigField = FieldSpec.builder(
                        fieldType,
                        fieldName,
                        Modifier.PRIVATE,
                        FINAL
                    ).build();

                    configurationClassBuilder.addField(nestedConfigField);

                    // Constructor statement
                    constructorBodyBuilder.addStatement(
                        "add($L = new $T(keys, $S, rootKey, changer, (p, k) -> new $T(p, k, rootKey, changer)))",
                        fieldName,
                        fieldType,
                        fieldName,
                        Utils.getConfigurationName((ClassName) baseType)
                    );
                }

                final Value valueAnnotation = field.getAnnotation(Value.class);
                if (valueAnnotation != null) {
                    // Must be a primitive or an array of the primitives (including java.lang.String)
                    if (!isPrimitiveOrArrayOfPrimitives(baseType)) {
                        throw new ProcessorException(
                            "@Value " + clazz.getQualifiedName() + "." + field.getSimpleName() + " field must" +
                                " have one of the following types: boolean, int, long, double, String or an array of " +
                                "aforementioned type."
                        );
                    }

                    // Create value (DynamicProperty<>) field
                    final FieldSpec generatedField = FieldSpec.builder(fieldType, fieldName, Modifier.PRIVATE, FINAL).build();

                    configurationClassBuilder.addField(generatedField);

                    final CodeBlock validatorsBlock = ValidationGenerator.generateValidators(field);

                    // Constructor statement
                    constructorBodyBuilder.addStatement(
                        "add($L = new $T(keys, $S, rootKey, changer), $L)",
                        fieldName, fieldType, fieldName, validatorsBlock
                    );
                }

                configDesc.getFields().add(new ConfigurationElement(fieldType, fieldName, viewClassType, initClassType, changeClassType));

                createGetters(configurationClassBuilder, configurationInterfaceBuilder, fieldName, types);
            }

            props.put(configClass, configDesc);

            // Create VIEW, INIT and CHANGE classes
            createPojoBindings(clazz, fields, schemaClassName, configurationClassBuilder, configurationInterfaceBuilder);

            if (isRoot) {
                TypeMirror storageType = null;

                try {
                    rootAnnotation.storage();
                }
                catch (MirroredTypesException e) {
                    storageType = e.getTypeMirrors().get(0);
                }

                createRootKeyField(configInterface, configurationInterfaceBuilder, configDesc, storageType, schemaClassName);
            }

            // Create constructors for configuration class
            createConstructors(configurationClassBuilder, constructorBodyBuilder);

            // Write configuration interface
            buildClass(packageName, configurationInterfaceBuilder.build());

            buildClass(packageName, configurationClassBuilder.build());
        }

        return true;
    }

    /** */
    private void createRootKeyField(ClassName configInterface,
        TypeSpec.Builder configurationClassBuilder,
        ConfigurationDescription configDesc,
        TypeMirror storageType,
        ClassName schemaClassName
    ) {
        ParameterizedTypeName fieldTypeName = ParameterizedTypeName.get(ClassName.get(RootKey.class), configInterface);

        ClassName nodeClassName = Utils.getNodeName(schemaClassName);

        FieldSpec keyField = FieldSpec.builder(
            fieldTypeName, "KEY", PUBLIC, STATIC, FINAL)
            .initializer(
                "$T.newRootKey($S, $T.class, $T::new, (rootKey, changer) -> new $T($T.emptyList(), $S, rootKey, changer))",
                ConfigurationRegistry.class, configDesc.getName(), storageType, nodeClassName,
                Utils.getConfigurationName(schemaClassName), Collections.class, configDesc.getName()
            )
            .build();

        configurationClassBuilder.addField(keyField);
    }

    /**
     * Create getters for configuration class.
     *
     * @param configurationClassBuilder
     * @param configurationInterfaceBuilder
     * @param fieldName
     * @param types
     */
    private void createGetters(
        TypeSpec.Builder configurationClassBuilder,
        TypeSpec.Builder configurationInterfaceBuilder,
        String fieldName,
        ConfigurationFieldTypes types
    ) {
        MethodSpec interfaceGetMethod = MethodSpec.methodBuilder(fieldName)
            .addModifiers(PUBLIC, ABSTRACT)
            .returns(types.getInterfaceGetMethodType())
            .build();
        configurationInterfaceBuilder.addMethod(interfaceGetMethod);

        MethodSpec.Builder getMethodBuilder = MethodSpec.methodBuilder(fieldName)
            .addAnnotation(Override.class)
            .addJavadoc(INHERIT_DOC)
            .addModifiers(PUBLIC, FINAL)
            .returns(types.getInterfaceGetMethodType());

        if (Utils.isNamedConfiguration(types.getFieldType())) {
            getMethodBuilder.addAnnotation(suppressWarningsUnchecked())
                .addStatement("return ($T)$L", NamedConfigurationTree.class, fieldName);
        }
        else
            getMethodBuilder.addStatement("return $L", fieldName);

        configurationClassBuilder.addMethod(getMethodBuilder.build());
    }

    /**
     * Get types for configuration classes generation.
     * @param field
     * @return Bundle with all types for configuration
     */
    private ConfigurationFieldTypes getTypes(final VariableElement field) {
        TypeName fieldType = null;
        TypeName interfaceGetMethodType = null;

        final TypeName baseType = TypeName.get(field.asType());

        TypeName unwrappedType = baseType;
        TypeName viewClassType = baseType;
        TypeName initClassType = baseType;
        TypeName changeClassType = baseType;

        final ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
        if (confAnnotation != null) {
            fieldType = Utils.getConfigurationName((ClassName) baseType);
            interfaceGetMethodType = Utils.getConfigurationInterfaceName((ClassName) baseType);

            unwrappedType = fieldType;
            viewClassType = Utils.getViewName((ClassName) baseType);
            initClassType = Utils.getInitName((ClassName) baseType);
            changeClassType = Utils.getChangeName((ClassName) baseType);
        }

        final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
        if (namedConfigAnnotation != null) {
            ClassName interfaceGetType = Utils.getConfigurationInterfaceName((ClassName) baseType);

            viewClassType = Utils.getViewName((ClassName) baseType);
            initClassType = Utils.getInitName((ClassName) baseType);
            changeClassType = Utils.getChangeName((ClassName) baseType);

            fieldType = ParameterizedTypeName.get(ClassName.get(NamedListConfiguration.class), interfaceGetType, viewClassType, changeClassType, initClassType);
            interfaceGetMethodType = ParameterizedTypeName.get(ClassName.get(NamedConfigurationTree.class), interfaceGetType, viewClassType, changeClassType, initClassType);
        }

        final Value valueAnnotation = field.getAnnotation(Value.class);
        if (valueAnnotation != null) {
            ClassName dynPropClass = ClassName.get(DynamicProperty.class);
            ClassName confValueClass = ClassName.get(ConfigurationValue.class);

            TypeName genericType = baseType;

            if (genericType.isPrimitive()) {
                genericType = genericType.box();
            }

            fieldType = ParameterizedTypeName.get(dynPropClass, genericType);
            interfaceGetMethodType = ParameterizedTypeName.get(confValueClass, genericType);
        }

        return new ConfigurationFieldTypes(fieldType, unwrappedType, viewClassType, initClassType, changeClassType, interfaceGetMethodType);
    }

    /**
     * Wrapper for configuration schema types.
     */
    private static class ConfigurationFieldTypes {
        /** Field get method type. */
        private final TypeName fieldType;

        /** Configuration type (if marked with @ConfigValue or @NamedConfig), or original type (if marked with @Value) */
        private final TypeName unwrappedType;

        /** VIEW object type. */
        private final TypeName viewClassType;

        /** INIT object type. */
        private final TypeName initClassType;

        /** CHANGE object type. */
        private final TypeName changeClassType;

        /** Get method type for public interface. */
        private final TypeName interfaceGetMethodType;

        private ConfigurationFieldTypes(TypeName fieldType, TypeName unwrappedType, TypeName viewClassType, TypeName initClassType, TypeName changeClassType, TypeName interfaceGetMethodType) {
            this.fieldType = fieldType;
            this.unwrappedType = unwrappedType;
            this.viewClassType = viewClassType;
            this.initClassType = initClassType;
            this.changeClassType = changeClassType;
            this.interfaceGetMethodType = interfaceGetMethodType;
        }

        /** */
        public TypeName getInterfaceGetMethodType() {
            return interfaceGetMethodType;
        }

        /** */
        public TypeName getFieldType() {
            return fieldType;
        }

        /** */
        public TypeName getUnwrappedType() {
            return unwrappedType;
        }

        /** */
        public TypeName getViewClassType() {
            return viewClassType;
        }

        /** */
        public TypeName getInitClassType() {
            return initClassType;
        }

        /** */
        public TypeName getChangeClassType() {
            return changeClassType;
        }
    }

    /**
     * Create configuration class constructors.
     *
     * @param configuratorClassName Configurator (configuration wrapper) class name.
     * @param copyConstructorBodyBuilder Copy constructor body.
     * @param configurationClassBuilder Configuration class builder.
     * @param constructorBodyBuilder Constructor body.
     */
    private void createConstructors(
        TypeSpec.Builder configurationClassBuilder,
        CodeBlock.Builder constructorBodyBuilder
    ) {
        final MethodSpec constructorWithName = MethodSpec.constructorBuilder()
            .addModifiers(PUBLIC)
            .addParameter(ParameterizedTypeName.get(List.class, String.class), "prefix")
            .addParameter(String.class, "key")
            .addParameter(ParameterizedTypeName.get(ClassName.get(RootKey.class), WILDCARD), "rootKey")
            .addParameter(ConfigurationChanger.class, "changer")
            .addStatement("super(prefix, key, rootKey, changer)")
            .addCode(constructorBodyBuilder.build())
            .build();
        configurationClassBuilder.addMethod(constructorWithName);
    }

    /**
     * Create VIEW, INIT and CHANGE classes and methods.
     * @param clazz Original class for the schema.
     * @param fields List of configuration fields.
     * @param schemaClassName Class name of schema.
     * @param configurationClassBuilder Configuration class builder.
     */
    private void createPojoBindings(
        TypeElement clazz,
        List<VariableElement> fields,
        ClassName schemaClassName,
        TypeSpec.Builder configurationClassBuilder,
        TypeSpec.Builder configurationInterfaceBuilder
    ) {
        final ClassName viewClassTypeName = Utils.getViewName(schemaClassName);
        final ClassName initClassName = Utils.getInitName(schemaClassName);
        final ClassName changeClassName = Utils.getChangeName(schemaClassName);

        ClassName dynConfClass = ClassName.get(DynamicConfiguration.class);
        TypeName dynConfViewClassType = ParameterizedTypeName.get(dynConfClass, viewClassTypeName, initClassName, changeClassName);

        configurationClassBuilder.superclass(dynConfViewClassType);

        ClassName confTreeInterface = ClassName.get(ConfigurationTree.class);
        TypeName confTreeParameterized = ParameterizedTypeName.get(confTreeInterface, viewClassTypeName, changeClassName);

        configurationInterfaceBuilder.addSuperinterface(confTreeParameterized);

        // This code will be refactored in the future. Right now I don't want to entangle it with existing code
        // generation. It has only a few considerable problems - hardcode and a lack of proper arrays handling.
        // Clone method should be used to guarantee data integrity.
        ClassName viewClsName = Utils.getViewName(schemaClassName);

        ClassName changeClsName = Utils.getChangeName(schemaClassName);

        ClassName initClsName = Utils.getInitName(schemaClassName);

        ClassName nodeClsName = Utils.getNodeName(schemaClassName);

        TypeSpec.Builder viewClsBuilder = TypeSpec.interfaceBuilder(viewClsName)
            .addModifiers(PUBLIC);

        TypeSpec.Builder changeClsBuilder = TypeSpec.interfaceBuilder(changeClsName)
            .addModifiers(PUBLIC);

        TypeSpec.Builder initClsBuilder = TypeSpec.interfaceBuilder(initClsName)
            .addModifiers(PUBLIC);

        TypeSpec.Builder nodeClsBuilder = TypeSpec.classBuilder(nodeClsName)
            .addModifiers(PUBLIC, FINAL)
            .superclass(ClassName.get(InnerNode.class))
            .addSuperinterface(viewClsName)
            .addSuperinterface(changeClsName)
            .addSuperinterface(initClsName)
            // Cannot use "schemaClassName" here because it can't handle inner static classes.
            .addField(FieldSpec.builder(ClassName.get(clazz), "_spec", PRIVATE, FINAL)
                .initializer("new $T()", ClassName.get(clazz))
                .build()
            );

        TypeVariableName t = TypeVariableName.get("T");

        MethodSpec.Builder traverseChildrenBuilder = MethodSpec.methodBuilder("traverseChildren")
            .addAnnotation(Override.class)
            .addJavadoc(INHERIT_DOC)
            .addModifiers(PUBLIC)
            .addTypeVariable(t)
            .returns(TypeName.VOID)
            .addParameter(ParameterizedTypeName.get(ClassName.get(ConfigurationVisitor.class), t), "visitor");

        MethodSpec.Builder traverseChildBuilder = MethodSpec.methodBuilder("traverseChild")
            .addAnnotation(Override.class)
            .addJavadoc(INHERIT_DOC)
            .addModifiers(PUBLIC)
            .addTypeVariable(t)
            .returns(t)
            .addException(NoSuchElementException.class)
            .addParameter(ClassName.get(String.class), "key")
            .addParameter(ParameterizedTypeName.get(ClassName.get(ConfigurationVisitor.class), t), "visitor")
            .beginControlFlow("switch (key)");

        MethodSpec.Builder constructBuilder = MethodSpec.methodBuilder("construct")
            .addAnnotation(Override.class)
            .addJavadoc(INHERIT_DOC)
            .addModifiers(PUBLIC)
            .returns(TypeName.VOID)
            .addException(NoSuchElementException.class)
            .addParameter(ClassName.get(String.class), "key")
            .addParameter(ClassName.get(ConfigurationSource.class), "src")
            .beginControlFlow("switch (key)");

        MethodSpec.Builder constructDefaultBuilder = MethodSpec.methodBuilder("constructDefault")
            .addAnnotation(Override.class)
            .addJavadoc(INHERIT_DOC)
            .addModifiers(PUBLIC)
            .returns(TypeName.BOOLEAN)
            .addException(NoSuchElementException.class)
            .addParameter(ClassName.get(String.class), "key")
            .beginControlFlow("switch (key)");

        MethodSpec.Builder schemaTypeBuilder = MethodSpec.methodBuilder("schemaType")
            .addAnnotation(Override.class)
            .addJavadoc(INHERIT_DOC)
            .addModifiers(PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Class.class), WILDCARD))
            .addStatement("return _spec.getClass()");

        ClassName consumerClsName = ClassName.get(Consumer.class);

        for (VariableElement field : fields) {
            Value valAnnotation = field.getAnnotation(Value.class);
            boolean mutable = valAnnotation == null || !valAnnotation.immutable();

            String fieldName = field.getSimpleName().toString();
            TypeName schemaFieldType = TypeName.get(field.asType());

            boolean isArray = schemaFieldType instanceof ArrayTypeName;

            boolean leafField = isPrimitiveOrArrayOfPrimitives(schemaFieldType)
                || !((ClassName)schemaFieldType).simpleName().contains("ConfigurationSchema");

            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

            TypeName viewFieldType = leafField ? schemaFieldType : ClassName.get(
                ((ClassName)schemaFieldType).packageName(),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "View")
            );

            TypeName changeFieldType = leafField ? schemaFieldType : ClassName.get(
                ((ClassName)schemaFieldType).packageName(),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "Change")
            );

            TypeName initFieldType = leafField ? schemaFieldType : ClassName.get(
                ((ClassName)schemaFieldType).packageName(),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "Init")
            );

            TypeName nodeFieldType = leafField ? schemaFieldType.box() : ClassName.get(
                ((ClassName)schemaFieldType).packageName() + (leafField ? "" : ".impl"),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "Node")
            );

            TypeName namedListParamType = nodeFieldType;

            if (namedListField) {
                viewFieldType = ParameterizedTypeName.get(ClassName.get(NamedListView.class), WildcardTypeName.subtypeOf(viewFieldType));

                nodeFieldType = ParameterizedTypeName.get(ClassName.get(NamedListNode.class), nodeFieldType);

                changeFieldType = ParameterizedTypeName.get(ClassName.get(NamedListChange.class), changeFieldType, initFieldType);

                initFieldType = ParameterizedTypeName.get(ClassName.get(NamedListInit.class), initFieldType);
            }

            {
                FieldSpec.Builder nodeFieldBuilder = FieldSpec.builder(nodeFieldType, fieldName, PRIVATE);

                if (namedListField)
                    nodeFieldBuilder.initializer("new $T<>($T::new)", NamedListNode.class, namedListParamType);

                nodeClsBuilder.addField(nodeFieldBuilder.build());
            }

            {
                {
                    MethodSpec.Builder getMtdBuilder = MethodSpec.methodBuilder(fieldName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(viewFieldType);

                    viewClsBuilder.addMethod(getMtdBuilder.build());
                }

                {
                    CodeBlock getStatement;

                    if (isArray)
                        getStatement = CodeBlock.builder().add("return $L.clone()", fieldName).build();
                    else
                        getStatement = CodeBlock.builder().add("return $L", fieldName).build();

                    MethodSpec.Builder nodeGetMtdBuilder = MethodSpec.methodBuilder(fieldName)
                        .addAnnotation(Override.class)
                        .addModifiers(PUBLIC)
                        .returns(leafField ? viewFieldType : nodeFieldType)
                        .addStatement(getStatement);

                    nodeClsBuilder.addMethod(nodeGetMtdBuilder.build());
                }
            }

            if (mutable) {
                String changeMtdName = "change" + capitalize(fieldName);

                {
                    MethodSpec.Builder changeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(changeClsName);

                    if (valAnnotation != null)
                        changeMtdBuilder.addParameter(changeFieldType, fieldName);
                    else
                        changeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), fieldName);

                    changeClsBuilder.addMethod(changeMtdBuilder.build());
                }

                {
                    MethodSpec.Builder nodeChangeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                        .addAnnotation(Override.class)
                        .addModifiers(PUBLIC)
                        .returns(nodeClsName);

                    if (valAnnotation != null) {
                        CodeBlock changeStatement;

                        if (isArray)
                            changeStatement = CodeBlock.builder().add("this.$L = $L.clone()", fieldName, fieldName).build();
                        else
                            changeStatement = CodeBlock.builder().add("this.$L = $L", fieldName, fieldName).build();

                        nodeChangeMtdBuilder
                            .addParameter(changeFieldType, fieldName)
                            .addStatement(changeStatement);
                    }
                    else {
                        String paramName = fieldName + "Consumer";
                        nodeChangeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), paramName);

                        if (!namedListField) {
                            nodeChangeMtdBuilder.addStatement(
                                "if ($L == null) $L = new $T()",
                                fieldName,
                                fieldName,
                                nodeFieldType
                            );
                            nodeChangeMtdBuilder.addStatement("$L.accept($L)", paramName, fieldName);
                        }
                        else {
                            nodeChangeMtdBuilder.addAnnotation(suppressWarningsUnchecked());

                            nodeChangeMtdBuilder.addStatement("$L.accept((NamedListChange)$L)", paramName, fieldName);
                        }
                    }

                    nodeChangeMtdBuilder.addStatement("return this");

                    nodeClsBuilder.addMethod(nodeChangeMtdBuilder.build());
                }
            }

            {
                String initMtdName = "init" + capitalize(fieldName);

                {
                    MethodSpec.Builder initMtdBuilder = MethodSpec.methodBuilder(initMtdName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(initClsName);

                    if (valAnnotation != null)
                        initMtdBuilder.addParameter(changeFieldType, fieldName);
                    else
                        initMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, initFieldType), fieldName);

                    initClsBuilder.addMethod(initMtdBuilder.build());
                }

                {
                    MethodSpec.Builder nodeInitMtdBuilder = MethodSpec.methodBuilder(initMtdName)
                        .addAnnotation(Override.class)
                        .addModifiers(PUBLIC)
                        .returns(nodeClsName);

                    if (valAnnotation != null) {
                        CodeBlock initStatement;

                        if (isArray)
                            initStatement = CodeBlock.builder().add("this.$L = $L.clone()", fieldName, fieldName).build();
                        else
                            initStatement = CodeBlock.builder().add("this.$L = $L", fieldName, fieldName).build();

                        nodeInitMtdBuilder
                            .addParameter(initFieldType, fieldName)
                            .addStatement(initStatement);
                    }
                    else {
                        String paramName = fieldName + "Consumer";
                        nodeInitMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, initFieldType), paramName);

                        if (!namedListField) {
                            nodeInitMtdBuilder.addStatement(
                                "if ($L == null) $L = new $T()",
                                fieldName,
                                fieldName,
                                nodeFieldType
                            );

                            nodeInitMtdBuilder.addStatement("$L.accept($L)", paramName, fieldName);
                        }
                        else {
                            nodeInitMtdBuilder.addAnnotation(suppressWarningsUnchecked());

                            nodeInitMtdBuilder.addStatement("$L.accept((NamedListChange)$L)", paramName, fieldName);
                        }
                    }

                    nodeInitMtdBuilder.addStatement("return this");

                    nodeClsBuilder.addMethod(nodeInitMtdBuilder.build());
                }
            }

            {
                if (leafField) {
                    traverseChildrenBuilder.addStatement("visitor.visitLeafNode($S, $L)", fieldName, fieldName);

                    traverseChildBuilder
                        .addStatement("case $S: return visitor.visitLeafNode(key, $L)", fieldName, fieldName);
                }
                else if (namedListField) {
                    traverseChildrenBuilder.addStatement("visitor.visitNamedListNode($S, $L)", fieldName, fieldName);

                    traverseChildBuilder
                        .addStatement("case $S: return visitor.visitNamedListNode(key, $L)", fieldName, fieldName);
                }
                else {
                    traverseChildrenBuilder.addStatement("visitor.visitInnerNode($S, $L)", fieldName, fieldName);

                    traverseChildBuilder
                        .addStatement("case $S: return visitor.visitInnerNode(key, $L)", fieldName, fieldName);
                }
            }

            {
                if (leafField) {
                    constructBuilder.addStatement(
                        "case $S: $L = src == null ? null : src.unwrap($T.class)",
                        fieldName,
                        fieldName,
                        schemaFieldType.box()
                    )
                    .addStatement(INDENT + "break");

                    if (valAnnotation.hasDefault()) {
                        constructDefaultBuilder
                            .addStatement("case $S: $L = _spec.$L", fieldName, fieldName, fieldName)
                            .addStatement(INDENT + "return true");
                    }
                    else
                        constructDefaultBuilder.addStatement("case $S: return false", fieldName);
                }
                else if (namedListField) {
                    constructBuilder
                        .addStatement(
                            "case $S: if (src == null) $L = new $T<>($T::new)",
                            fieldName,
                            fieldName,
                            NamedListNode.class,
                            namedListParamType
                        )
                        .addStatement(
                            INDENT + "else src.descend($L = $L.copy())",
                            fieldName,
                            fieldName
                        )
                        .addStatement(INDENT + "break");
                }
                else {
                    constructBuilder
                        .addStatement(
                            "case $S: if (src == null) $L = null",
                            fieldName,
                            fieldName
                        )
                        .addStatement(
                            INDENT + "else src.descend($L = ($L == null ? new $T() : ($T)$L.copy()))",
                            fieldName,
                            fieldName,
                            nodeFieldType,
                            nodeFieldType,
                            fieldName
                        )
                        .addStatement(INDENT + "break");
                }
            }
        }

        traverseChildBuilder
            .addStatement("default: throw new $T(key)", NoSuchElementException.class)
            .endControlFlow();

        constructBuilder
            .addStatement("default: throw new $T(key)", NoSuchElementException.class)
            .endControlFlow();

        constructDefaultBuilder
            .addStatement("default: throw new $T(key)", NoSuchElementException.class)
            .endControlFlow();

        nodeClsBuilder
            .addMethod(traverseChildrenBuilder.build())
            .addMethod(traverseChildBuilder.build())
            .addMethod(constructBuilder.build())
            .addMethod(constructDefaultBuilder.build())
            .addMethod(schemaTypeBuilder.build());

        TypeSpec viewCls = viewClsBuilder.build();
        TypeSpec changeCls = changeClsBuilder.build();
        TypeSpec initCls = initClsBuilder.build();
        TypeSpec nodeCls = nodeClsBuilder.build();

        buildClass(viewClsName.packageName(), viewCls);
        buildClass(changeClsName.packageName(), changeCls);
        buildClass(initClsName.packageName(), initCls);
        buildClass(nodeClsName.packageName(), nodeCls);
    }

    /** */
    private void buildClass(String packageName, TypeSpec cls) {
        try {
            JavaFile.builder(packageName, cls)
                .indent(INDENT)
                .build()
                .writeTo(filer);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to generate class " + packageName + "." + cls.name, e);
        }
    }

    /** */
    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Checks whether TypeName is a primitive (or String) or an array of primitives (or Strings)
     * @param typeName TypeName.
     * @return {@code true} if type is primitive or array.
     */
    private boolean isPrimitiveOrArrayOfPrimitives(TypeName typeName) {
        String type = typeName.toString();

        if (typeName instanceof ArrayTypeName)
            type = ((ArrayTypeName) typeName).componentType.toString();

        switch (type) {
            case "boolean":
            case "int":
            case "long":
            case "double":
            case "java.lang.String":
                return true;

            default:
                return false;

        }
    }

    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Set.of(Config.class.getCanonicalName(), ConfigurationRoot.class.getCanonicalName());
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_11;
    }
}
