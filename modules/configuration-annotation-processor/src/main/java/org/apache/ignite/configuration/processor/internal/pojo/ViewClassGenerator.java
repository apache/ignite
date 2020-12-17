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

import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.internal.NamedList;
import org.apache.ignite.configuration.processor.internal.Utils;

/**
 * VIEW object class generator.
 */
public class ViewClassGenerator extends ClassGenerator {
    /** Constructor. */
    public ViewClassGenerator(ProcessingEnvironment env) {
        super(env);
    }

    /** {@inheritDoc} */
    @Override protected FieldMapping mapField(VariableElement field) {
        final ConfigValue configAnnotation = field.getAnnotation(ConfigValue.class);
        final NamedConfigValue namedConfigAnnotation = field.getAnnotation(NamedConfigValue.class);

        final TypeMirror type = field.asType();
        String name = field.getSimpleName().toString();

        TypeName fieldType = TypeName.get(type);

        if (fieldType.isPrimitive())
            fieldType = fieldType.box();

        if (namedConfigAnnotation != null || configAnnotation != null) {
            ClassName confClass = (ClassName) fieldType;
            fieldType = Utils.getViewName(confClass);

            if (namedConfigAnnotation != null)
                fieldType = ParameterizedTypeName.get(ClassName.get(NamedList.class), fieldType);
        }

        final FieldSpec fieldSpec = FieldSpec.builder(fieldType, name, Modifier.PRIVATE, Modifier.FINAL).build();

        return new FieldMapping(field, fieldSpec);
    }

    /** {@inheritDoc} */
    @Override protected MethodSpec mapMethod(ClassName clazz, FieldSpec field) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected MethodSpec createConstructor(List<FieldSpec> fields) {
        return Utils.createConstructor(fields);
    }
}
