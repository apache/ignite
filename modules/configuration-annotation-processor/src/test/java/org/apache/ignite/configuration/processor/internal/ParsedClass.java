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

import com.google.common.base.Functions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import spoon.reflect.declaration.CtClass;
import spoon.reflect.declaration.CtConstructor;
import spoon.reflect.declaration.CtExecutable;
import spoon.reflect.declaration.CtMethod;
import spoon.reflect.declaration.CtParameter;
import spoon.reflect.reference.CtFieldReference;
import spoon.reflect.reference.CtReference;

/**
 * Convenient wrapper for parsed source file.
 * Method and constructor signatures are represented by string containing name and list of argument types.
 * E.g.: "foo(java.lang.Integer, java.lang.String)" for method {@code public Object foo(Integer a, String b)}.
 */
public class ParsedClass {
    /** Class info. */
    private final CtClass<?> cls;

    /** Class fields by name. */
    private final Map<String, CtFieldReference<?>> fields;

    /** Class methods by signature. */
    private final Map<String, CtMethod<?>> methods;

    /** Class constructors by signature. */
    private final Map<String, CtConstructor<?>> constructors;

    /**
     * Constructor.
     * @param cls Class info.
     */
    public ParsedClass(CtClass<?> cls) {
        this.cls = cls;

        this.fields = cls.getAllFields()
            .stream()
            .collect(Collectors.toMap(CtReference::getSimpleName, Functions.identity()));

        this.methods = cls.getMethods()
            .stream()
            .collect(Collectors.toMap(this::getMethodName, Functions.identity()));

        this.constructors = cls.getConstructors()
            .stream()
            .collect(Collectors.toMap(this::getMethodName, Functions.identity()));
    }

    /**
     * Get method name from method meta info.
     * @param method Method meta info.
     * @return Method name.
     */
    private String getMethodName(CtExecutable<?> method) {
        final List<CtParameter<?>> parameters = method.getParameters();

        final String params = parameters.stream().map(parameter ->
            parameter.getType().getQualifiedName()).collect(Collectors.joining(", ")
        );

        return method.getSimpleName() + "(" + params + ")";
    }

    /**
     * Get fields.
     * @return Fields.
     */
    public Map<String, CtFieldReference<?>> getFields() {
        return fields;
    }

    /**
     * Get methods.
     * @return Methods.
     */
    public Map<String, CtMethod<?>> getMethods() {
        return methods;
    }

    /**
     * Get constructors.
     * @return Constructors.
     */
    public Map<String, CtConstructor<?>> getConstructors() {
        return constructors;
    }
}
