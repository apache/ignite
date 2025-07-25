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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

/**
 * Annotation processor that generates serialization and deserialization code for classes implementing the {@code Message} interface.
 * <p>
 * This processor scans all {@code Message} classes and generates a corresponding serializer class that contains
 * {@code writeTo} and {@code readFrom} methods.
 * <p>
 * The generated serializer follows the naming convention: {@code [MessageClassName]Serializer}.
 * <p>
 * Only fields annotated with {@link Order} and accessed through matching accessor methods
 * (i.e., {@code fieldName()} and {@code fieldName(value)}) are included in the serialization logic.
 * <p>
 * <strong>Usage Requirements:</strong>
 * <ul>
 *   <li>The target class must implement the {@code Message} interface.</li>
 *   <li>Each field to be serialized must be annotated with {@code @Order}.</li>
 *   <li>By default, each serializing field must have a getter named {@code fieldName()} and
 *   a setter named {@code fieldName(value)}.</li>
 *   <li>If {@link Order#method()} attribute was set, then each serializing field
 *   must have a getter named {@code method()} and a setter named {@code method(value)}.</li>
 * </ul>
 *
 * <p>
 * This processor is typically registered using the {@code META-INF/services/javax.annotation.processing.Processor}
 * service file and triggered during the compilation phase.
 */
@SupportedAnnotationTypes("org.apache.ignite.internal.Order")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class MessageProcessor extends AbstractProcessor {
    /** Base interface that every message must implement. */
    static final String MESSAGE_INTERFACE = "org.apache.ignite.plugin.extensions.communication.Message";

    /**
     * Processes all classes implementing the {@code Message} interface and generates corresponding serializer code.
     */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        TypeMirror msgType = processingEnv.getElementUtils().getTypeElement(MESSAGE_INTERFACE).asType();

        Map<TypeElement, List<VariableElement>> msgFields = new HashMap<>();

        for (Element el: roundEnv.getRootElements()) {
            if (el.getKind() != ElementKind.CLASS)
                continue;

            TypeElement clazz = (TypeElement)el;

            if (!processingEnv.getTypeUtils().isAssignable(clazz.asType(), msgType))
                continue;

            if (clazz.getModifiers().contains(Modifier.ABSTRACT))
                continue;

            List<VariableElement> fields = orderedFields(clazz);

            if (!fields.isEmpty())
                msgFields.put(clazz, fields);
        }

        for (Map.Entry<TypeElement, List<VariableElement>> type: msgFields.entrySet()) {
            try {
                new MessageSerializerGenerator(processingEnv).generate(type.getKey(), type.getValue());
            }
            catch (Exception e) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate a message serializer:" + e.getMessage(),
                    type.getKey());
            }
        }

        return true;
    }

    /**
     * Collects all fields annotated with {@link Order} from the given {@link TypeElement} and all its superclasses.
     * <p>
     * The resulting list is sorted in ascending order of the {@code @Order} value.
     *
     * @param type the {@code TypeElement} representing the class to inspect.
     * @return a list of {@code VariableElement} objects representing all ordered fields, including those declared in superclasses.
     */
    private List<VariableElement> orderedFields(TypeElement type) {
        List<VariableElement> result = new ArrayList<>();

        while (type != null) {
            for (Element el: type.getEnclosedElements()) {
                if (el.getAnnotation(Order.class) != null) {
                    result.add((VariableElement)el);

                    if (el.getModifiers().contains(Modifier.STATIC)) {
                        processingEnv.getMessager().printMessage(
                            Diagnostic.Kind.ERROR,
                            "Annotation @Order must be used only for non-static fields.",
                            el);
                    }
                }
            }

            Element superType = processingEnv.getTypeUtils().asElement(type.getSuperclass());

            type = (TypeElement)superType;
        }

        result.sort(Comparator.comparingInt(f -> f.getAnnotation(Order.class).value()));

        for (int i = 0; i < result.size(); i++) {
            if (result.get(i).getAnnotation(Order.class).value() != i) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Annotation @Order must be a sequence from 0 to " + (result.size() - 1),
                    result.get(i));
            }
        }

        return result;
    }
}
