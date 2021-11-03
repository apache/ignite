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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import org.jetbrains.annotations.Nullable;

/**
 * Various shortcuts over the {@link Types} utilities.
 */
public class TypeUtils {
    /**
     *
     */
    private final Types types;

    /**
     *
     */
    private final Elements elements;

    /**
     * @param processingEnvironment processing environment
     */
    public TypeUtils(ProcessingEnvironment processingEnvironment) {
        this.types = processingEnvironment.getTypeUtils();
        this.elements = processingEnvironment.getElementUtils();
    }

    /**
     * Returns {@code true} if the <i>erasure</i> of the given types are actually the same type.
     *
     * @param type1 first type (represented by a mirror)
     * @param type2 second type (represented by a {@code Class})
     * @return {@code true} if the erasure of both types represent the same type, {@code false} otherwise.
     */
    public boolean isSameType(TypeMirror type1, Class<?> type2) {
        TypeMirror type2Mirror = typeMirrorFromClass(type2);

        return types.isSameType(erasure(type1), erasure(type2Mirror));
    }

    /**
     * Returns the primitive type represented by its boxed value or {@code null} if the given type is not a boxed primitive type.
     *
     * @param type boxed wrapper of a primitive type
     * @return corresponding primitive type
     */
    @Nullable
    public PrimitiveType unboxedType(TypeMirror type) {
        try {
            return types.unboxedType(type);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    /**
     * Returns {@code true} if the given type element implements the given interface (represented by its {@link Class}) either directly or
     * indirectly.
     *
     * @param element element which parent interfaces are to be inspected
     * @param cls     target superinterface to search for
     * @return {@code true} if the given {@code element} is a subtype of {@code cls}
     */
    public boolean hasSuperInterface(TypeElement element, Class<?> cls) {
        return allInterfaces(element).anyMatch(e -> isSameType(e.asType(), cls));
    }

    /**
     * Creates a stream of elements representing all superinterfaces of the given element, including the element itself.
     *
     * @param start starting element for exploring the inheritance hierarchy
     * @return stream of superinterfaces
     */
    public Stream<TypeElement> allInterfaces(TypeElement start) {
        // perform BFS to explore all superinterfaces
        var queue = new ArrayDeque<TypeElement>();

        return Stream.iterate(start, Objects::nonNull, currentElement -> {
            currentElement.getInterfaces().stream()
                    .map(types::asElement)
                    .map(TypeElement.class::cast)
                    .forEach(queue::add);

            return queue.poll();
        });
    }

    /**
     * Shortcut for the {@link Types#erasure(TypeMirror)} method.
     */
    private TypeMirror erasure(TypeMirror type) {
        return types.erasure(type);
    }

    /**
     * Creates a {@link TypeMirror} represented by the given {@link Class}.
     */
    private TypeMirror typeMirrorFromClass(Class<?> cls) {
        return elements
                .getTypeElement(cls.getCanonicalName())
                .asType();
    }
}
