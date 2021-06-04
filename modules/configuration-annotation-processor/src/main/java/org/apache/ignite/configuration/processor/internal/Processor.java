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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.tree.NamedListChange;
import org.apache.ignite.configuration.tree.NamedListView;

import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

/**
 * Annotation processor that produces configuration classes.
 */
public class Processor extends AbstractProcessor {
    /** Java file padding. */
    private static final String INDENT = "    ";

    /** */
    private static final ClassName ROOT_KEY_CLASSNAME = ClassName.get("org.apache.ignite.configuration", "RootKey");

    /**
     * Constructor.
     */
    public Processor() {
    }

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        try {
            return process0(roundEnvironment);
        } catch (Throwable t) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Failed to process configuration: " + sw.toString());
        }
        return false;
    }

    /**
     * Processes a set of annotation types on type elements.
     * @param roundEnvironment Processing environment.
     * @return Whether or not the set of annotation types are claimed by this processor.
     */
    private boolean process0(RoundEnvironment roundEnvironment) {
        final Elements elementUtils = processingEnv.getElementUtils();

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

            final ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());

            // Get name for generated configuration interface.
            final ClassName configInterface = Utils.getConfigurationInterfaceName(schemaClassName);

            TypeSpec.Builder configurationInterfaceBuilder = TypeSpec.interfaceBuilder(configInterface)
                .addModifiers(PUBLIC);

            for (VariableElement field : fields) {
                if (field.getModifiers().contains(STATIC))
                    continue;

                if (!field.getModifiers().contains(PUBLIC))
                    throw new ProcessorException("Field " + clazz.getQualifiedName() + "." + field + " must be public");

                Element fieldTypeElement = processingEnv.getTypeUtils().asElement(field.asType());

                // Get original field type (must be another configuration schema or "primitive" like String or long)
                final TypeName baseType = TypeName.get(field.asType());

                final String fieldName = field.getSimpleName().toString();

                // Get configuration types (VIEW, CHANGE and so on)
                TypeName interfaceGetMethodType = getInterfaceGetMethodType(field);

                final ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
                if (confAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @ConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }
                }

                final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
                if (namedConfigAnnotation != null) {
                    if (fieldTypeElement.getAnnotation(Config.class) == null) {
                        throw new ProcessorException(
                            "Class for @NamedConfigValue field must be defined as @Config: " +
                                clazz.getQualifiedName() + "." + field.getSimpleName()
                        );
                    }
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
                }

                createGetters(configurationInterfaceBuilder, fieldName, interfaceGetMethodType);
            }

            // Create VIEW and CHANGE classes
            createPojoBindings(fields, schemaClassName, configurationInterfaceBuilder);

            if (isRoot)
                createRootKeyField(configInterface, configurationInterfaceBuilder, schemaClassName, clazz);

            // Write configuration interface
            buildClass(packageName, configurationInterfaceBuilder.build());
        }

        return true;
    }

    /** */
    private void createRootKeyField(
        ClassName configInterface,
        TypeSpec.Builder configurationClassBuilder,
        ClassName schemaClassName,
        TypeElement realSchemaClass
    ) {
        ClassName viewClassName = Utils.getViewName(schemaClassName);

        ParameterizedTypeName fieldTypeName = ParameterizedTypeName.get(ROOT_KEY_CLASSNAME, configInterface, viewClassName);

        FieldSpec keyField = FieldSpec.builder(fieldTypeName, "KEY", PUBLIC, STATIC, FINAL)
            .initializer(
                "new $T($T.class)",
                ROOT_KEY_CLASSNAME,
                realSchemaClass
            )
            .build();

        configurationClassBuilder.addField(keyField);
    }

    /**
     * Create getters for configuration class.
     *
     * @param configurationInterfaceBuilder
     * @param fieldName
     * @param types
     */
    private void createGetters(
        TypeSpec.Builder configurationInterfaceBuilder,
        String fieldName,
        TypeName interfaceGetMethodType
    ) {
        MethodSpec interfaceGetMethod = MethodSpec.methodBuilder(fieldName)
            .addModifiers(PUBLIC, ABSTRACT)
            .returns(interfaceGetMethodType)
            .build();

        configurationInterfaceBuilder.addMethod(interfaceGetMethod);
    }

    /**
     * Get types for configuration classes generation.
     * @param field
     * @return Bundle with all types for configuration
     */
    private TypeName getInterfaceGetMethodType(final VariableElement field) {
        TypeName interfaceGetMethodType = null;

        final TypeName baseType = TypeName.get(field.asType());

        final ConfigValue confAnnotation = field.getAnnotation(ConfigValue.class);
        if (confAnnotation != null) {
            interfaceGetMethodType = Utils.getConfigurationInterfaceName((ClassName) baseType);
        }

        final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);
        if (namedConfigAnnotation != null) {
            ClassName interfaceGetType = Utils.getConfigurationInterfaceName((ClassName) baseType);

            TypeName viewClassType = Utils.getViewName((ClassName) baseType);
            TypeName changeClassType = Utils.getChangeName((ClassName) baseType);

            interfaceGetMethodType = ParameterizedTypeName.get(ClassName.get(NamedConfigurationTree.class), interfaceGetType, viewClassType, changeClassType);
        }

        final Value valueAnnotation = field.getAnnotation(Value.class);
        if (valueAnnotation != null) {
            // It is necessary to use class names without loading classes so that we won't
            // accidentally get NoClassDefFoundError
            ClassName confValueClass = ClassName.get("org.apache.ignite.configuration", "ConfigurationValue");

            TypeName genericType = baseType;

            if (genericType.isPrimitive()) {
                genericType = genericType.box();
            }

            interfaceGetMethodType = ParameterizedTypeName.get(confValueClass, genericType);
        }

        return interfaceGetMethodType;
    }

    /**
     * Create VIEW and CHANGE classes and methods.
     * @param fields List of configuration fields.
     * @param schemaClassName Class name of schema.
     */
    private void createPojoBindings(
        List<VariableElement> fields,
        ClassName schemaClassName,
        TypeSpec.Builder configurationInterfaceBuilder
    ) {
        final ClassName viewClassTypeName = Utils.getViewName(schemaClassName);
        final ClassName changeClassName = Utils.getChangeName(schemaClassName);

        ClassName confTreeInterface = ClassName.get("org.apache.ignite.configuration", "ConfigurationTree");
        TypeName confTreeParameterized = ParameterizedTypeName.get(confTreeInterface, viewClassTypeName, changeClassName);

        configurationInterfaceBuilder.addSuperinterface(confTreeParameterized);

        // This code will be refactored in the future. Right now I don't want to entangle it with existing code
        // generation. It has only a few considerable problems - hardcode and a lack of proper arrays handling.
        // Clone method should be used to guarantee data integrity.
        ClassName viewClsName = Utils.getViewName(schemaClassName);

        ClassName changeClsName = Utils.getChangeName(schemaClassName);

        TypeSpec.Builder viewClsBuilder = TypeSpec.interfaceBuilder(viewClsName)
            .addModifiers(PUBLIC);

        TypeSpec.Builder changeClsBuilder = TypeSpec.interfaceBuilder(changeClsName)
            .addSuperinterface(viewClsName)
            .addModifiers(PUBLIC);

        ClassName consumerClsName = ClassName.get(Consumer.class);

        for (VariableElement field : fields) {
            Value valAnnotation = field.getAnnotation(Value.class);

            String fieldName = field.getSimpleName().toString();
            TypeName schemaFieldType = TypeName.get(field.asType());

            boolean leafField = isPrimitiveOrArrayOfPrimitives(schemaFieldType)
                || !((ClassName)schemaFieldType).simpleName().contains("ConfigurationSchema");

            boolean namedListField = field.getAnnotation(NamedConfigValue.class) != null;

            TypeName viewFieldType = leafField ? schemaFieldType : Utils.getViewName((ClassName)schemaFieldType);

            TypeName changeFieldType = leafField ? schemaFieldType : Utils.getChangeName((ClassName)schemaFieldType);

            if (namedListField) {
                viewFieldType = ParameterizedTypeName.get(ClassName.get(NamedListView.class), WildcardTypeName.subtypeOf(viewFieldType));

                changeFieldType = ParameterizedTypeName.get(ClassName.get(NamedListChange.class), changeFieldType);
            }

            {
                MethodSpec.Builder getMtdBuilder = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(PUBLIC, ABSTRACT)
                    .returns(viewFieldType);

                viewClsBuilder.addMethod(getMtdBuilder.build());
            }

            {
                String changeMtdName = "change" + capitalize(fieldName);

                {
                    MethodSpec.Builder changeMtdBuilder = MethodSpec.methodBuilder(changeMtdName)
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(changeClsName);

                    if (valAnnotation != null) {
                        if (schemaFieldType instanceof ArrayTypeName)
                            changeMtdBuilder.varargs(true);

                        changeMtdBuilder.addParameter(changeFieldType, fieldName);
                    }
                    else
                        changeMtdBuilder.addParameter(ParameterizedTypeName.get(consumerClsName, changeFieldType), fieldName);

                    changeClsBuilder.addMethod(changeMtdBuilder.build());
                }
            }
        }

        TypeSpec viewCls = viewClsBuilder.build();
        TypeSpec changeCls = changeClsBuilder.build();

        buildClass(viewClsName.packageName(), viewCls);
        buildClass(changeClsName.packageName(), changeCls);
    }

    /** */
    private void buildClass(String packageName, TypeSpec cls) {
        try {
            JavaFile.builder(packageName, cls)
                .indent(INDENT)
                .build()
                .writeTo(processingEnv.getFiler());
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
        return SourceVersion.latest();
    }
}
