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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.NamedListConfiguration;

/**
 * Annotation processing utilities.
 */
public class Utils {
    /** Private constructor. */
    private Utils() {
    }

    /**
     * Create constructor for
     *
     * @param fieldSpecs List of fields.
     * @return Constructor method.
     */
    public static MethodSpec createConstructor(List<FieldSpec> fieldSpecs) {
        final MethodSpec.Builder builder = MethodSpec.constructorBuilder();

        for (FieldSpec field : fieldSpecs) {
            builder.addParameter(field.type, field.name);
            builder.addStatement("this.$L = $L", field.name, field.name);
        }

        return builder.build();
    }

    /**
     * Create getters for fields.
     *
     * @param fieldSpecs List of fields.
     * @return List of getter methods.
     */
    public static List<MethodSpec> createGetters(List<FieldSpec> fieldSpecs) {
        return fieldSpecs.stream().map(field ->
            MethodSpec.methodBuilder(field.name)
                .returns(field.type)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("return $L", field.name)
                .build()).collect(Collectors.toList()
        );
    }

    /**
     * Create builder-style setters.
     *
     * @param fieldSpecs List of fields.
     * @return List of setter methods.
     */
    public static List<MethodSpec> createBuildSetters(List<FieldSpec> fieldSpecs) {
        return fieldSpecs.stream().map(field -> {
            return MethodSpec.methodBuilder("with" + field.name)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("this.$L = $L", field.name, field.name)
                .build();
        }).collect(Collectors.toList());
    }

    /**
     * Create '{@code new SomeObject(arg1, arg2, ..., argN)}' code block.
     *
     * @param type Type of the new object.
     * @param fieldSpecs List of arguments.
     * @return New object code block.
     */
    public static CodeBlock newObject(TypeName type, List<VariableElement> fieldSpecs) {
        String args = fieldSpecs.stream().map(f -> f.getSimpleName().toString()).collect(Collectors.joining(", "));
        return CodeBlock.builder()
            .add("new $T($L)", type, args)
            .build();
    }

    /**
     * Get class with parameters, boxing them if necessary.
     *
     * @param clz Generic class.
     * @param types Generic parameters.
     * @return Parameterized type.
     */
    public static ParameterizedTypeName getParameterized(ClassName clz, TypeName... types) {
        types = Arrays.stream(types).map(t -> {
            if (t.isPrimitive())
                t = t.box();
            return t;
        }).toArray(TypeName[]::new);
        return ParameterizedTypeName.get(clz, types);
    }

    /**
     * Get {@link ClassName} for configuration class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration ClassName.
     */
    public static ClassName getConfigurationName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("Schema", "Impl")
        );
    }

    /**
     * Get {@link ClassName} for configuration class' public interface.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration's public interface ClassName.
     */
    public static ClassName getConfigurationInterfaceName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("Schema", "")
        );
    }

    /**
     * Get {@link ClassName} for configuration VIEW object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration VIEW object ClassName.
     */
    public static ClassName getViewName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "")
        );
    }

    /**
     * Get {@link ClassName} for configuration INIT object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration INIT object ClassName.
     */
    public static ClassName getInitName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            "Init" + schemaClassName.simpleName().replace("ConfigurationSchema", "")
        );
    }

    /**
     * Get {@link ClassName} for configuration CHANGE object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration CHANGE object ClassName.
     */
    public static ClassName getChangeName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            "Change" + schemaClassName.simpleName().replace("ConfigurationSchema", "")
        );
    }

    /**
     * Check whether type is {@link NamedListConfiguration}.
     *
     * @param type Type.
     * @return {@code true} if type is {@link NamedListConfiguration}.
     */
    public static boolean isNamedConfiguration(TypeName type) {
        if (type instanceof ParameterizedTypeName) {
            ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) type;

            if (parameterizedTypeName.rawType.equals(ClassName.get(NamedListConfiguration.class)))
                return true;
        }
        return false;
    }

    /**
     * Get {@code DynamicConfiguration} inside of the named configuration.
     *
     * @param type Type name.
     * @return {@link DynamicConfiguration} class name.
     */
    public static TypeName unwrapNamedListConfigurationClass(TypeName type) {
        if (type instanceof ParameterizedTypeName) {
            ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) type;

            if (parameterizedTypeName.rawType.equals(ClassName.get(NamedListConfiguration.class)))
                return parameterizedTypeName.typeArguments.get(1);
        }

        throw new ProcessorException(type + " is not a NamedListConfiguration class");
    }

}
