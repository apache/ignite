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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import javax.lang.model.util.Elements;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.Configurator;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.DynamicProperty;
import org.apache.ignite.configuration.internal.NamedListConfiguration;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;
import org.apache.ignite.configuration.internal.selector.Selector;
import org.apache.ignite.configuration.internal.validation.MemberKey;
import org.apache.ignite.configuration.processor.internal.pojo.ChangeClassGenerator;
import org.apache.ignite.configuration.processor.internal.pojo.InitClassGenerator;
import org.apache.ignite.configuration.processor.internal.pojo.ViewClassGenerator;
import org.apache.ignite.configuration.processor.internal.validation.ValidationGenerator;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListChange;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.NamedListView;

import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

/**
 * Annotation processor that produces configuration classes.
 */
public class Processor extends AbstractProcessor {
    /** Java file padding. */
    private static final String INDENT = "    ";

    /** Wildcard (?) TypeName. */
    private static final TypeName WILDCARD = WildcardTypeName.subtypeOf(Object.class);

    /** Type of Configurator (every DynamicConfiguration has a Configurator field). */
    private static final ParameterizedTypeName CONFIGURATOR_TYPE = ParameterizedTypeName.get(
        ClassName.get(Configurator.class),
        WildcardTypeName.subtypeOf(
            ParameterizedTypeName.get(ClassName.get(DynamicConfiguration.class), WILDCARD, WILDCARD, WILDCARD)
        )
    );

    /** Generator of VIEW classes. */
    private ViewClassGenerator viewClassGenerator;

    /** Generator of INIT classes. */
    private InitClassGenerator initClassGenerator;

    /** Generator of CHANGE classes. */
    private ChangeClassGenerator changeClassGenerator;

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
        viewClassGenerator = new ViewClassGenerator(processingEnv);
        initClassGenerator = new InitClassGenerator(processingEnv);
        changeClassGenerator = new ChangeClassGenerator(processingEnv);
    }

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        final Elements elementUtils = processingEnv.getElementUtils();

        Map<TypeName, ConfigurationDescription> props = new HashMap<>();

        List<ConfigurationDescription> roots = new ArrayList<>();

        // Package to use for Selectors and Keys classes
        String packageForUtil = "";

        // All classes annotated with @Config
        final Set<TypeElement> annotatedConfigs = roundEnvironment.getElementsAnnotatedWith(Config.class).stream()
            .filter(element -> element.getKind() == ElementKind.CLASS)
            .map(TypeElement.class::cast)
            .collect(Collectors.toSet());

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

            final Config classConfigAnnotation = clazz.getAnnotation(Config.class);

            // Configuration name
            final String configName = classConfigAnnotation.value();
            // Is root of the configuration
            final boolean isRoot = classConfigAnnotation.root();
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
            if (isRoot) {
                roots.add(configDesc);
                packageForUtil = packageName;
            }

            TypeSpec.Builder configurationClassBuilder = TypeSpec.classBuilder(configClass)
                .addSuperinterface(configInterface)
                .addModifiers(PUBLIC, FINAL);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                .addModifiers(PUBLIC);

            CodeBlock.Builder constructorBodyBuilder = CodeBlock.builder();
            CodeBlock.Builder copyConstructorBodyBuilder = CodeBlock.builder();

            for (VariableElement field : fields) {
                Element fieldTypeElement = processingEnv.getTypeUtils().asElement(field.asType());

                // Get original field type (must be another configuration schema or "primitive" like String or long)
                final TypeName baseType = TypeName.get(field.asType());

                final String fieldName = field.getSimpleName().toString();

                // Get configuration types (VIEW, INIT, CHANGE and so on)
                final ConfigurationFieldTypes types = getTypes(field);

                TypeName getMethodType = types.getGetMethodType();
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
                            .builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL)
                            .build();

                    configurationClassBuilder.addField(nestedConfigField);

                    // Constructor statement
                    constructorBodyBuilder.addStatement("add($L = new $T(qualifiedName, $S, false, configurator, this.root))", fieldName, getMethodType, fieldName);

                    // Copy constructor statement
                    copyConstructorBodyBuilder.addStatement("add($L = base.$L.copy(this.root))", fieldName, fieldName);
                }

                final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
                if (namedConfigAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @NamedConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }

                    ClassName fieldType = Utils.getConfigurationName((ClassName) baseType);

                    // Create NamedListConfiguration<> field
                    final FieldSpec nestedConfigField = FieldSpec.builder(
                        getMethodType,
                        fieldName,
                        Modifier.PRIVATE,
                        FINAL
                    ).build();

                    configurationClassBuilder.addField(nestedConfigField);

                    // Constructor statement
                    constructorBodyBuilder.addStatement(
                        "add($L = new $T(qualifiedName, $S, configurator, this.root, (p, k) -> new $T(p, k, true, configurator, this.root)))",
                        fieldName,
                        getMethodType,
                        fieldName,
                        fieldType
                    );

                    // Copy constructor statement
                    copyConstructorBodyBuilder.addStatement("add($L = base.$L.copy(this.root))", fieldName, fieldName);
                }

                final Value valueAnnotation = field.getAnnotation(Value.class);
                if (valueAnnotation != null) {
                    switch (baseType.toString()) {
                        case "boolean":
                        case "int":
                        case "long":
                        case "double":
                        case "java.lang.String":
                            break;

                        default:
                            throw new ProcessorException(
                                "@Value " + clazz.getQualifiedName() + "." + field.getSimpleName() + " field must" +
                                    " have one of the following types: boolean, int, long, double, String."
                            );
                    }

                    // Create value (DynamicProperty<>) field
                    final FieldSpec generatedField = FieldSpec.builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL).build();

                    configurationClassBuilder.addField(generatedField);

                    final CodeBlock validatorsBlock = ValidationGenerator.generateValidators(field);

                    // Constructor statement
                    constructorBodyBuilder.addStatement(
                        "add($L = new $T(qualifiedName, $S, new $T($T.class, $S), this.configurator, this.root), $L)",
                        fieldName, getMethodType, fieldName, MemberKey.class, configClass, fieldName, validatorsBlock
                    );

                    // Copy constructor statement
                    copyConstructorBodyBuilder.addStatement("add($L = base.$L.copy(this.root))", fieldName, fieldName);
                }

                configDesc.getFields().add(new ConfigurationElement(getMethodType, fieldName, viewClassType, initClassType, changeClassType));

                createGettersAndSetter(configurationClassBuilder, configurationInterfaceBuilder, fieldName, types, valueAnnotation);
            }

            props.put(configClass, configDesc);

            // Create VIEW, INIT and CHANGE classes
            createPojoBindings(packageName, fields, schemaClassName, configurationClassBuilder, configurationInterfaceBuilder);

            if (isRoot)
                createRootKeyField(configInterface, configurationInterfaceBuilder, configDesc);

            // Create constructors for configuration class
            createConstructors(configClass, configName, configurationClassBuilder, CONFIGURATOR_TYPE, constructorBodyBuilder, copyConstructorBodyBuilder);

            // Create copy method for configuration class
            createCopyMethod(configClass, configurationClassBuilder);

            // Write configuration interface
            JavaFile interfaceFile = JavaFile.builder(packageName, configurationInterfaceBuilder.build()).build();

            try {
                interfaceFile.writeTo(filer);
            } catch (IOException e) {
                throw new ProcessorException("Failed to create configuration class " + configClass.toString(), e);
            }

            // Write configuration
            JavaFile classFile = JavaFile.builder(packageName, configurationClassBuilder.build()).build();

            try {
                classFile.writeTo(filer);
            } catch (IOException e) {
                throw new ProcessorException("Failed to create configuration class " + configClass.toString(), e);
            }
        }

        // Get all generated configuration nodes
        final List<ConfigurationNode> flattenConfig = roots.stream()
            .map((ConfigurationDescription cfg) -> buildConfigForest(cfg, props))
            .flatMap(Set::stream)
            .collect(Collectors.toList());

        // Generate Keys class
        createKeysClass(packageForUtil, flattenConfig);

        // Generate Selectors class
        createSelectorsClass(packageForUtil, flattenConfig);

        return true;
    }

    /** */
    private void createRootKeyField(ClassName configInterface,
        TypeSpec.Builder configurationClassBuilder,
        ConfigurationDescription configDesc) {

        ParameterizedTypeName fieldTypeName = ParameterizedTypeName.get(ClassName.get(RootKey.class), configInterface);

        TypeSpec anonymousClass = TypeSpec.anonymousClassBuilder("")
            .addSuperinterface(fieldTypeName)
            .addMethod(MethodSpec
                .methodBuilder("key")
                .addAnnotation(Override.class)
                .addModifiers(PUBLIC)
                .returns(TypeName.get(String.class))
                .addStatement("return $S", configDesc.getName())
                .build()).build();

        FieldSpec keyField = FieldSpec.builder(
            fieldTypeName, "KEY", PUBLIC, STATIC, FINAL)
            .initializer("$L", anonymousClass)
            .build();

        configurationClassBuilder.addField(keyField);
    }

    /**
     * Create getters and setters for configuration class.
     *
     * @param configurationClassBuilder
     * @param configurationInterfaceBuilder
     * @param fieldName
     * @param types
     * @param valueAnnotation
     */
    private void createGettersAndSetter(
        TypeSpec.Builder configurationClassBuilder,
        TypeSpec.Builder configurationInterfaceBuilder,
        String fieldName,
        ConfigurationFieldTypes types,
        Value valueAnnotation
    ) {
        MethodSpec interfaceGetMethod = MethodSpec.methodBuilder(fieldName)
            .addModifiers(PUBLIC, ABSTRACT)
            .returns(types.getInterfaceGetMethodType())
            .build();
        configurationInterfaceBuilder.addMethod(interfaceGetMethod);

        MethodSpec getMethod = MethodSpec.methodBuilder(fieldName)
            .addModifiers(PUBLIC, FINAL)
            .returns(types.getGetMethodType())
            .addStatement("return $L", fieldName)
            .build();
        configurationClassBuilder.addMethod(getMethod);

        if (valueAnnotation != null) {
            MethodSpec setMethod = MethodSpec
                .methodBuilder(fieldName)
                .addModifiers(PUBLIC, FINAL)
                .addParameter(types.getUnwrappedType(), fieldName)
                .addStatement("this.$L.change($L)", fieldName, fieldName)
                .build();
            configurationClassBuilder.addMethod(setMethod);
        }
    }

    /**
     * Get types for configuration classes generation.
     * @param field
     * @return Bundle with all types for configuration
     */
    private ConfigurationFieldTypes getTypes(final VariableElement field) {
        TypeName getMethodType = null;
        TypeName interfaceGetMethodType = null;

        final TypeName baseType = TypeName.get(field.asType());

        TypeName unwrappedType = baseType;
        TypeName viewClassType = baseType;
        TypeName initClassType = baseType;
        TypeName changeClassType = baseType;

        final ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
        if (confAnnotation != null) {
            getMethodType = Utils.getConfigurationName((ClassName) baseType);
            interfaceGetMethodType = Utils.getConfigurationInterfaceName((ClassName) baseType);

            unwrappedType = getMethodType;
            viewClassType = Utils.getViewName((ClassName) baseType);
            initClassType = Utils.getInitName((ClassName) baseType);
            changeClassType = Utils.getChangeName((ClassName) baseType);
        }

        final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
        if (namedConfigAnnotation != null) {
            ClassName fieldType = Utils.getConfigurationName((ClassName) baseType);

            viewClassType = Utils.getViewName((ClassName) baseType);
            initClassType = Utils.getInitName((ClassName) baseType);
            changeClassType = Utils.getChangeName((ClassName) baseType);

            getMethodType = ParameterizedTypeName.get(ClassName.get(NamedListConfiguration.class), viewClassType, fieldType, initClassType, changeClassType);
            interfaceGetMethodType = ParameterizedTypeName.get(ClassName.get(NamedListConfiguration.class), viewClassType, fieldType, initClassType, changeClassType);
        }

        final Value valueAnnotation = field.getAnnotation(Value.class);
        if (valueAnnotation != null) {
            ClassName dynPropClass = ClassName.get(DynamicProperty.class);
            ClassName confValueClass = ClassName.get(ConfigurationValue.class);

            TypeName genericType = baseType;

            if (genericType.isPrimitive()) {
                genericType = genericType.box();
            }

            getMethodType = ParameterizedTypeName.get(dynPropClass, genericType);
            interfaceGetMethodType = ParameterizedTypeName.get(confValueClass, genericType);
        }

        return new ConfigurationFieldTypes(getMethodType, unwrappedType, viewClassType, initClassType, changeClassType, interfaceGetMethodType);
    }

    /**
     * Wrapper for configuration schema types.
     */
    private static class ConfigurationFieldTypes {
        /** Field get method type. */
        private final TypeName getMethodType;

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

        public ConfigurationFieldTypes(TypeName getMethodType, TypeName unwrappedType, TypeName viewClassType, TypeName initClassType, TypeName changeClassType, TypeName interfaceGetMethodType) {
            this.getMethodType = getMethodType;
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
        public TypeName getGetMethodType() {
            return getMethodType;
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
     * Create copy-method for configuration class.
     *
     * @param configClass Configuration class name.
     * @param configurationClassBuilder Configuration class builder.
     */
    private void createCopyMethod(ClassName configClass, TypeSpec.Builder configurationClassBuilder) {
        MethodSpec copyMethod = MethodSpec.methodBuilder("copy")
            .addAnnotation(Override.class)
            .addModifiers(PUBLIC)
            .addParameter(DynamicConfiguration.class, "root")
            .returns(configClass)
            .addStatement("return new $T(this, root)", configClass)
            .build();

        configurationClassBuilder.addMethod(copyMethod);
    }

    /**
     * Create configuration class constructors.
     *
     * @param configClass Configuration class name.
     * @param configName Configuration name.
     * @param configurationClassBuilder Configuration class builder.
     * @param configuratorClassName Configurator (configuration wrapper) class name.
     * @param constructorBodyBuilder Constructor body.
     * @param copyConstructorBodyBuilder Copy constructor body.
     */
    private void createConstructors(
        ClassName configClass,
        String configName,
        TypeSpec.Builder configurationClassBuilder,
        ParameterizedTypeName configuratorClassName,
        CodeBlock.Builder constructorBodyBuilder,
        CodeBlock.Builder copyConstructorBodyBuilder
    ) {
        final MethodSpec constructorWithName = MethodSpec.constructorBuilder()
            .addModifiers(PUBLIC)
            .addParameter(String.class, "prefix")
            .addParameter(String.class, "key")
            .addParameter(boolean.class, "isNamed")
            .addParameter(configuratorClassName, "configurator")
            .addParameter(DynamicConfiguration.class, "root")
            .addStatement("super(prefix, key, isNamed, configurator, root)")
            .addCode(constructorBodyBuilder.build())
            .build();
        configurationClassBuilder.addMethod(constructorWithName);

        final MethodSpec copyConstructor = MethodSpec.constructorBuilder()
            .addModifiers(PRIVATE)
            .addParameter(configClass, "base")
            .addParameter(DynamicConfiguration.class, "root")
            .addStatement("super(base.prefix, base.key, base.isNamed, base.configurator, root)")
            .addCode(copyConstructorBodyBuilder.build())
            .build();
        configurationClassBuilder.addMethod(copyConstructor);

        final MethodSpec emptyConstructor = MethodSpec.constructorBuilder()
            .addModifiers(PUBLIC)
            .addParameter(configuratorClassName, "configurator")
            .addStatement("this($S, $S, false, configurator, null)", "", configName)
            .build();

        configurationClassBuilder.addMethod(emptyConstructor);
    }

    /**
     * Create selectors.
     *
     * @param packageForUtil Package to place selectors class to.
     * @param flattenConfig List of configuration nodes.
     */
    private void createSelectorsClass(String packageForUtil, List<ConfigurationNode> flattenConfig) {
        ClassName selectorsClassName = ClassName.get(packageForUtil, "Selectors");

        final TypeSpec.Builder selectorsClass = TypeSpec.classBuilder(selectorsClassName)
            .superclass(BaseSelectors.class)
            .addModifiers(PUBLIC, FINAL);

        final CodeBlock.Builder selectorsStaticBlockBuilder = CodeBlock.builder();
        selectorsStaticBlockBuilder.addStatement("$T publicLookup = $T.publicLookup()", MethodHandles.Lookup.class, MethodHandles.class);

        selectorsStaticBlockBuilder.beginControlFlow("try");

        // For every configuration node create selector (based on a method call chain)
        for (ConfigurationNode configNode : flattenConfig) {
            String regex = "([a-z])([A-Z]+)";
            String replacement = "$1_$2";

            // Selector variable name (like LOCAL_BASELINE_AUTO_ADJUST_ENABLED)
            final String varName = configNode.getName()
                .replaceAll(regex, replacement)
                .toUpperCase()
                .replace(".", "_");

            TypeName type = configNode.getType();

            if (Utils.isNamedConfiguration(type))
                type = Utils.unwrapNamedListConfigurationClass(type);

            StringBuilder methodCall = new StringBuilder();

            ConfigurationNode current = configNode;
            ConfigurationNode root = null;
            int namedCount = 0;

            // Walk from node up to the root and create a method call chain
            while (current != null) {
                boolean isNamed = false;

                if (Utils.isNamedConfiguration(current.getType())) {
                    namedCount++;
                    isNamed = true;
                }

                if (current.getParent() != null) {
                    String newMethodCall = "." + current.getOriginalName() + "()";
                    // if config is named, then create a call with name parameter
                    if (isNamed)
                        newMethodCall += ".get(name" + (namedCount - 1) + ")";

                    methodCall.insert(0, newMethodCall);
                } else
                    root = current;

                current = current.getParent();
            }

            TypeName selectorRec = Utils.getParameterized(ClassName.get(Selector.class), root.getType(), type, configNode.getView(), configNode.getInit(), configNode.getChange());

            if (namedCount > 0) {
                final MethodSpec.Builder builder = MethodSpec.methodBuilder(varName);

                for (int i = 0; i < namedCount; i++) {
                    builder.addParameter(String.class, "name" + i);
                }

                selectorsClass.addMethod(
                    builder
                        .returns(selectorRec)
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .addStatement("return (root) -> root$L", methodCall.toString())
                        .build()
                );

                // Build a list of parameters for statement
                List<Object> params = new ArrayList<>();
                params.add(MethodHandle.class);
                params.add(varName);
                params.add(selectorsClassName);
                params.add(varName);
                params.add(MethodType.class);
                params.add(Selector.class);

                // For every named config in call chain -- add String (name) parameter
                for (int i = 0; i < namedCount; i++) {
                    params.add(String.class);
                }

                // Create a string for name parameters
                final String nameStringParameters = IntStream.range(0, namedCount).mapToObj(i -> "$T.class").collect(Collectors.joining(","));

                selectorsStaticBlockBuilder.addStatement("$T $L = publicLookup.findStatic($T.class, $S, $T.methodType($T.class, " + nameStringParameters + "))", params.toArray());

                selectorsStaticBlockBuilder.addStatement("put($S, $L)", configNode.getName(), varName);
            }
            else {
                selectorsClass.addField(
                    FieldSpec.builder(selectorRec, varName)
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .initializer("(root) -> root$L", methodCall.toString())
                        .build()
                );
                selectorsStaticBlockBuilder.addStatement("put($S, $L)", configNode.getName(), varName);
            }
        }

        selectorsStaticBlockBuilder
            .nextControlFlow("catch ($T e)", Exception.class)
            .endControlFlow();

        selectorsClass.addStaticBlock(selectorsStaticBlockBuilder.build());

        JavaFile selectorsClassFile = JavaFile.builder(selectorsClassName.packageName(), selectorsClass.build()).build();
        try {
            selectorsClassFile.writeTo(filer);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class: " + e.getMessage(), e);
        }
    }

    /**
     * Create keys class.
     *
     * @param packageForUtil Package to place keys class to.
     * @param flattenConfig List of configuration nodes.
     */
    private void createKeysClass(String packageForUtil, List<ConfigurationNode> flattenConfig) {
        final TypeSpec.Builder keysClass = TypeSpec.classBuilder("Keys").addModifiers(PUBLIC, FINAL);

        for (ConfigurationNode node : flattenConfig) {
            final String varName = node.getName().toUpperCase().replace(".", "_");
            keysClass.addField(
                FieldSpec.builder(String.class, varName)
                    .addModifiers(PUBLIC, STATIC, FINAL)
                    .initializer("$S", node.getName())
                    .build()
            );
        }

        JavaFile keysClassFile = JavaFile.builder(packageForUtil, keysClass.build()).build();
        try {
            keysClassFile.writeTo(filer);
        } catch (IOException e) {
            throw new ProcessorException("Failed to write class: " + e.getMessage(), e);
        }
    }

    /**
     * Create VIEW, INIT and CHANGE classes and methods.
     *
     * @param packageName Configuration package name.
     * @param fields List of configuration fields.
     * @param schemaClassName Class name of schema.
     * @param configurationClassBuilder Configuration class builder.
     */
    private void createPojoBindings(
        String packageName,
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

        try {
            viewClassGenerator.create(packageName, viewClassTypeName, fields);
            final MethodSpec toViewMethod = createToViewMethod(viewClassTypeName, fields);
            configurationClassBuilder.addMethod(toViewMethod);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class " + viewClassTypeName.toString(), e);
        }

        try {
            changeClassGenerator.create(packageName, changeClassName, fields);
            final MethodSpec changeMethod = createChangeMethod(changeClassName, fields);
            configurationClassBuilder.addMethod(changeMethod);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class " + changeClassName.toString(), e);
        }

        try {
            initClassGenerator.create(packageName, initClassName, fields);
            final MethodSpec initMethod = createInitMethod(initClassName, fields);
            configurationClassBuilder.addMethod(initMethod);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to write class " + initClassName.toString(), e);
        }

        // This code will be refactored in the future. Right now I don't want to entangle it with existing code
        // generation. It has only a few considerable problems - hardcode and a lack of proper arrays handling.
        // Clone method should be used to guarantee data integrity.
        ClassName viewClsName = ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "View")
        );

        ClassName changeClsName = ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "Change")
        );

        ClassName initClsName = ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "Init")
        );

        ClassName nodeClsName = ClassName.get(
            schemaClassName.packageName() + ".impl",
            schemaClassName.simpleName().replace("ConfigurationSchema", "Node")
        );

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
            .addSuperinterface(initClsName);

        MethodSpec.Builder traverseChildrenBuilder = MethodSpec.methodBuilder("traverseChildren")
            .addAnnotation(Override.class)
            .addJavadoc("{@inheritDoc}")
            .addModifiers(PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(ClassName.get(ConfigurationVisitor.class), "visitor");

        MethodSpec.Builder traverseChildBuilder = MethodSpec.methodBuilder("traverseChild")
            .addAnnotation(Override.class)
            .addJavadoc("{@inheritDoc}")
            .addModifiers(PUBLIC)
            .returns(TypeName.VOID)
            .addException(NoSuchElementException.class)
            .addParameter(ClassName.get(String.class), "key")
            .addParameter(ClassName.get(ConfigurationVisitor.class), "visitor")
            .beginControlFlow("switch (key)");

        ClassName consumerClsName = ClassName.get(Consumer.class);

        for (VariableElement field : fields) {
            Value valAnnotation = field.getAnnotation(Value.class);
            boolean immutable = valAnnotation != null && valAnnotation.immutable();

            String fieldName = field.getSimpleName().toString();
            TypeName schemaFieldType = TypeName.get(field.asType());

            boolean leafField = schemaFieldType.isPrimitive() || !((ClassName)schemaFieldType).simpleName().contains("ConfigurationSchema");
            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

            TypeName viewFieldType = schemaFieldType.isPrimitive() ? schemaFieldType : ClassName.get(
                ((ClassName)schemaFieldType).packageName(),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "View")
            );

            TypeName changeFieldType = schemaFieldType.isPrimitive() ? schemaFieldType : ClassName.get(
                ((ClassName)schemaFieldType).packageName(),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "Change")
            );

            TypeName initFieldType = schemaFieldType.isPrimitive() ? schemaFieldType : ClassName.get(
                ((ClassName)schemaFieldType).packageName(),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "Init")
            );

            TypeName nodeFieldType = schemaFieldType.isPrimitive() ? schemaFieldType.box() : ClassName.get(
                ((ClassName)schemaFieldType).packageName() + (leafField ? "" : ".impl"),
                ((ClassName)schemaFieldType).simpleName().replace("ConfigurationSchema", "Node")
            );

            if (namedListField) {
                viewFieldType = ParameterizedTypeName.get(ClassName.get(NamedListView.class), WildcardTypeName.subtypeOf(viewFieldType));

                changeFieldType = ParameterizedTypeName.get(ClassName.get(NamedListChange.class), changeFieldType);

                initFieldType = ParameterizedTypeName.get(ClassName.get(NamedListChange.class), initFieldType);

                nodeFieldType = ParameterizedTypeName.get(ClassName.get(NamedListNode.class), nodeFieldType);
            }

            {
                FieldSpec.Builder nodeFieldBuilder = FieldSpec.builder(nodeFieldType, fieldName, PRIVATE);

                if (namedListField)
                    nodeFieldBuilder.initializer("new $T<>($T::new)", NamedListNode.class, ((ParameterizedTypeName)nodeFieldType).typeArguments.get(0));

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
                    MethodSpec.Builder nodeGetMtdBuilder = MethodSpec.methodBuilder(fieldName)
                        .addAnnotation(Override.class)
                        .addModifiers(PUBLIC)
                        .returns(leafField ? viewFieldType : nodeFieldType)
                        .addStatement("return $L", fieldName); //TODO Explicit null check?

                    nodeClsBuilder.addMethod(nodeGetMtdBuilder.build());
                }
            }

            if (!immutable) {
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
                        .returns(changeClsName);

                    if (valAnnotation != null) {
                        nodeChangeMtdBuilder
                            .addParameter(changeFieldType, fieldName)
                            .addStatement("this.$L = $L", fieldName, fieldName);
                    }
                    else {
                        String paramName = fieldName + "Consumer";
                        nodeChangeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), paramName);

                        if (!namedListField) {
                            nodeChangeMtdBuilder.addStatement(
                                "$L = $L == null ? new $T() : $L",
                                fieldName,
                                fieldName,
                                nodeFieldType,
                                fieldName
                            );
                            nodeChangeMtdBuilder.addStatement("$L.accept($L)", paramName, fieldName);
                        }
                        else {
                            nodeChangeMtdBuilder.addAnnotation(
                                AnnotationSpec.builder(SuppressWarnings.class)
                                    .addMember("value", "$S", "unchecked")
                                    .build()
                            );

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
                        .returns(initClsName);

                    if (valAnnotation != null) {
                        nodeInitMtdBuilder
                            .addParameter(initFieldType, fieldName)
                            .addStatement("this.$L = $L", fieldName, fieldName);
                    }
                    else {
                        String paramName = fieldName + "Consumer";
                        nodeInitMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, initFieldType), paramName);

                        if (!namedListField) {
                            nodeInitMtdBuilder.addStatement(
                                "$L = $L == null ? new $T() : $L",
                                fieldName,
                                fieldName,
                                nodeFieldType,
                                fieldName
                            );

                            nodeInitMtdBuilder.addStatement("$L.accept($L)", paramName, fieldName);
                        }
                        else {
                            nodeInitMtdBuilder.addAnnotation(
                                AnnotationSpec.builder(SuppressWarnings.class)
                                    .addMember("value", "$S", "unchecked")
                                    .build()
                            );

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
                        .addStatement("case $S: visitor.visitLeafNode($S, $L)", fieldName, fieldName, fieldName)
                        .addStatement(INDENT + "break");
                }
                else if (namedListField) {
                    traverseChildrenBuilder.addStatement("visitor.visitNamedListNode($S, $L)", fieldName, fieldName);

                    traverseChildBuilder
                        .addStatement("case $S: visitor.visitNamedListNode($S, $L)", fieldName, fieldName, fieldName)
                        .addStatement(INDENT + "break");
                }
                else {
                    traverseChildrenBuilder.addStatement("visitor.visitInnerNode($S, $L)", fieldName, fieldName);

                    traverseChildBuilder
                        .addStatement("case $S: visitor.visitInnerNode($S, $L)", fieldName, fieldName, fieldName)
                        .addStatement(INDENT + "break");
                }
            }
        }

        traverseChildBuilder
            .addStatement("default: throw new $T(key)", NoSuchElementException.class)
            .endControlFlow();

        nodeClsBuilder
            .addMethod(traverseChildrenBuilder.build())
            .addMethod(traverseChildBuilder.build());

        TypeSpec viewCls = viewClsBuilder.build();
        TypeSpec changeCls = changeClsBuilder.build();
        TypeSpec initCls = initClsBuilder.build();
        TypeSpec nodeCls = nodeClsBuilder.build();

        try {
            buildClass(viewClsName, viewCls);
            buildClass(changeClsName, changeCls);
            buildClass(initClsName, initCls);
            buildClass(nodeClsName, nodeCls);
        }
        catch (IOException e) {
            throw new ProcessorException("Failed to generate classes", e);
        }
    }

    /** */
    private void buildClass(ClassName viewClsName, TypeSpec viewCls) throws IOException {
        JavaFile.builder(viewClsName.packageName(), viewCls)
            .indent(INDENT)
            .build()
            .writeTo(filer);
    }

    /** */
    private static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Build configuration forest base on root configuration description and all processed configurations.
     *
     * @param root Root configuration description.
     * @param props All configurations.
     * @return All possible config trees.
     */
    private Set<ConfigurationNode> buildConfigForest(ConfigurationDescription root, Map<TypeName, ConfigurationDescription> props) {
        Set<ConfigurationNode> res = new HashSet<>();
        Deque<ConfigurationNode> propsStack = new LinkedList<>();

        ConfigurationNode rootNode = new ConfigurationNode(root.getType(), root.getName(), root.getName(), root.getView(), root.getInit(), root.getChange(), null);

        propsStack.addFirst(rootNode);

        // Walk through the all fields of every node and build a tree of configuration (more like chain)
        while (!propsStack.isEmpty()) {
            final ConfigurationNode current = propsStack.pollFirst();

            // Get configuration type
            TypeName type = current.getType();

            if (Utils.isNamedConfiguration(type))
                type = Utils.unwrapNamedListConfigurationClass(current.getType());

            final ConfigurationDescription configDesc = props.get(type);

            // Get fields of configuration
            final List<ConfigurationElement> propertiesList = configDesc.getFields();

            if (current.getName() != null && !current.getName().isEmpty())
                // Add current node to result
                res.add(current);

            for (ConfigurationElement property : propertiesList) {
                String qualifiedName = property.getName();

                if (current.getName() != null && !current.getName().isEmpty())
                    qualifiedName = current.getName() + "." + qualifiedName;

                final ConfigurationNode newChainElement = new ConfigurationNode(
                    property.getType(),
                    qualifiedName,
                    property.getName(),
                    property.getView(),
                    property.getInit(),
                    property.getChange(),
                    current
                );

                boolean isNamedConfig = false;
                if (property.getType() instanceof ParameterizedTypeName) {
                    final ParameterizedTypeName parameterized = (ParameterizedTypeName) property.getType();

                    if (parameterized.rawType.equals(ClassName.get(NamedListConfiguration.class)))
                        isNamedConfig = true;
                }

                if (props.containsKey(property.getType()) || isNamedConfig)
                    // If it's not a leaf, add to stack
                    propsStack.add(newChainElement);
                else
                    // otherwise, add to result
                    res.add(newChainElement);

            }
        }
        return res;
    }

    /**
     * Create {@link org.apache.ignite.configuration.ConfigurationProperty#value} method for configuration class.
     *
     * @param type VIEW method type.
     * @param variables List of VIEW object's fields.
     * @return toView() method.
     */
    public MethodSpec createToViewMethod(TypeName type, List<VariableElement> variables) {
        String args = variables.stream()
            .map(v -> v.getSimpleName().toString() + ".value()")
            .collect(Collectors.joining(", "));

        final CodeBlock returnBlock = CodeBlock.builder()
            .add("return new $T($L)", type, args)
            .build();

        return MethodSpec.methodBuilder("value")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .returns(type)
            .addStatement(returnBlock)
            .build();
    }

    /**
     * Create {@link org.apache.ignite.configuration.internal.Modifier#init(Object)} method (accepts INIT object) for configuration class.
     *
     * @param type INIT method type.
     * @param variables List of INIT object's fields.
     * @return Init method.
     */
    public MethodSpec createInitMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        for (VariableElement variable : variables) {
            final String name = variable.getSimpleName().toString();
            builder.beginControlFlow("if (initial.$L() != null)", name);
            builder.addStatement("$L.init(initial.$L())", name, name);
            builder.endControlFlow();
        }

        return MethodSpec.methodBuilder("init")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .addParameter(type, "initial")
            .addCode(builder.build())
            .build();
    }

    /**
     * Create {@link org.apache.ignite.configuration.internal.Modifier#change(Object)} method (accepts CHANGE object) for configuration class.
     *
     * @param type CHANGE method type.
     * @param variables List of CHANGE object's fields.
     * @return Change method.
     */
    public MethodSpec createChangeMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        for (VariableElement variable : variables) {
            final Value valueAnnotation = variable.getAnnotation(Value.class);
            if (valueAnnotation != null && valueAnnotation.immutable())
                continue;

            final String name = variable.getSimpleName().toString();
            builder.beginControlFlow("if (changes.$L() != null)", name);
            builder.addStatement("$L.changeWithoutValidation(changes.$L())", name, name);
            builder.endControlFlow();
        }

        return MethodSpec.methodBuilder("changeWithoutValidation")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .addParameter(type, "changes")
            .addCode(builder.build())
            .build();
    }

    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Config.class.getCanonicalName());
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_11;
    }
}
