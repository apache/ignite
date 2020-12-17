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

package org.apache.ignite.configuration.processor.internal.pojo;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.configuration.processor.internal.Utils;

/**
 * Base POJO generator
 */
public abstract class ClassGenerator {
    /** Processing environment. */
    protected final ProcessingEnvironment env;

    /** Annotation processing filer. */
    private final Filer filer;

    /** Constructor. */
    public ClassGenerator(ProcessingEnvironment env) {
        this.env = env;
        this.filer = env.getFiler();
    }

    /**
     * Create class.
     *
     * @param packageName Package name for class.
     * @param className Class name.
     * @param fields List of fields.
     * @throws IOException If failed to write class file.
     */
    public final void create(String packageName, ClassName className, List<VariableElement> fields) throws IOException {
        TypeSpec.Builder classBuilder = TypeSpec
            .classBuilder(className)
            .addSuperinterface(Serializable.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        List<FieldMapping> fieldMappings = fields.stream().map(this::mapField).filter(Objects::nonNull).collect(Collectors.toList());

        generate(classBuilder, packageName, className, fieldMappings);

        final TypeSpec viewClass = classBuilder.build();
        JavaFile classFile = JavaFile.builder(packageName, viewClass).build();
        classFile.writeTo(filer);
    }

    /**
     * Generate class fields, methods and constructor.
     *
     * @param classBuilder Class builder.
     * @param packageName Package name.
     * @param className Class name.
     * @param fieldMappings Fields' mappings.
     */
    protected void generate(TypeSpec.Builder classBuilder, String packageName, ClassName className, List<FieldMapping> fieldMappings) {
        List<FieldSpec> fieldSpecs = fieldMappings.stream().map(FieldMapping::getFieldSpec).collect(Collectors.toList());

        List<MethodSpec> methodSpecs = fieldSpecs.stream().map(field -> mapMethod(className, field)).filter(Objects::nonNull).collect(Collectors.toList());

        classBuilder.addFields(fieldSpecs);
        classBuilder.addMethods(methodSpecs);

        final MethodSpec constructor = createConstructor(fieldSpecs);
        if (constructor != null)
            classBuilder.addMethod(constructor);

        final List<MethodSpec> getters = Utils.createGetters(fieldSpecs);

        classBuilder.addMethods(getters);
    }

    /**
     * Create {@link FieldSpec} from {@link VariableElement}.
     *
     * @param field Configuration class field.
     * @return Mapping.
     */
    protected abstract FieldMapping mapField(VariableElement field);

    /**
     * Create access methods for field.
     *
     * @param clazz Method return type.
     * @param field Configuration class field.
     * @return Method specs.
     */
    protected abstract MethodSpec mapMethod(ClassName clazz, FieldSpec field);

    /**
     * Create constructor from fields.
     *
     * @param fields Configuration fields.
     * @return If null, constructor won't be created.
     */
    protected MethodSpec createConstructor(List<FieldSpec> fields) {
        return null;
    }

}
