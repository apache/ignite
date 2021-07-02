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

package org.apache.ignite.internal.network.processor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import com.squareup.javapoet.ClassName;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * A wrapper around a {@link TypeElement} and the corresponding {@link ClassName} of an annotated Network Message.
 */
public class MessageClass {
    /** Annotated element. */
    private final TypeElement element;

    /** Class name of the {@code element}. */
    private final ClassName className;

    /** Annotation present on the {@code element}. */
    private final Transferable annotation;

    /**
     * Getter methods declared in the annotated interface.
     *
     * @see Transferable
     */
    private final List<ExecutableElement> getters;

    /**
     * @param messageElement element marked with the {@link Transferable} annotation.
     */
    MessageClass(ProcessingEnvironment processingEnv, TypeElement messageElement) {
        element = messageElement;
        className = ClassName.get(messageElement);
        annotation = messageElement.getAnnotation(Transferable.class);
        getters = extractGetters(processingEnv, messageElement);

        if (annotation.value() < 0)
            throw new ProcessingException("Message type must not be negative", null, element);
    }

    /**
     * Finds all getters in the given element and all of its superinterfaces.
     */
    private static List<ExecutableElement> extractGetters(ProcessingEnvironment processingEnv, TypeElement element) {
        var typeUtils = new TypeUtils(processingEnv);

        Map<String, ExecutableElement> gettersByName = typeUtils.allInterfaces(element)
            // this algorithm is suboptimal, since we have to scan over the same interfaces over and over again,
            // but this shouldn't be an issue, because it is not expected to have deep inheritance hierarchies
            .filter(e -> typeUtils.hasSuperInterface(e, NetworkMessage.class))
            // remove the NetworkMessage interface itself
            .filter(e -> !typeUtils.isSameType(e.asType(), NetworkMessage.class))
            .flatMap(e -> e.getEnclosedElements().stream())
            .filter(e -> e.getKind() == ElementKind.METHOD)
            // use a tree map to sort getters by name and remove duplicates
            .collect(Collectors.toMap(
                e -> e.getSimpleName().toString(),
                ExecutableElement.class::cast,
                (e1, e2) -> {
                    throw new ProcessingException(
                        String.format("Getter with name '%s' is already defined", e2.getSimpleName()), null, e2
                    );
                },
                TreeMap::new
            ));

        return List.copyOf(gettersByName.values());
    }

    /**
     * @return annotated element
     */
    public TypeElement element() {
        return element;
    }

    /**
     * @return class name of the {@link #element()}
     */
    public ClassName className() {
        return className;
    }

    /**
     * @return package name of the {@link #element()}
     */
    public String packageName() {
        return className.packageName();
    }

    /**
     * @return simple name of the {@link #element()}
     */
    public String simpleName() {
        return className.simpleName();
    }

    /**
     * @return getter methods declared in the annotated interface
     */
    public List<ExecutableElement> getters() {
        return getters;
    }

    /**
     * @return class name that the generated Network Message implementation should have
     */
    public ClassName implClassName() {
        return ClassName.get(packageName(), simpleName() + "Impl");
    }

    /**
     * @return class name that the generated Builder interface should have
     */
    public ClassName builderClassName() {
        return ClassName.get(packageName(), simpleName() + "Builder");
    }

    /**
     * @return name of the factory method that should be used by the message factories
     */
    public String asMethodName() {
        return decapitalize(simpleName());
    }

    /**
     * @return {@link Transferable#value()}
     */
    public short messageType() {
        return annotation.value();
    }

    /**
     * @return {@link Transferable#autoSerializable()}
     */
    public boolean isAutoSerializable() {
        return annotation.autoSerializable();
    }

    /**
     * @return a copy of the given string with the first character converted to lower case
     */
    private static String decapitalize(String str) {
        return Character.toLowerCase(str.charAt(0)) + str.substring(1);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MessageClass aClass = (MessageClass)o;
        return element.equals(aClass.element);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(element);
    }
}
