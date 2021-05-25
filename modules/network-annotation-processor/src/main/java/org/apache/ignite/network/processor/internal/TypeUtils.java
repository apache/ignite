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

package org.apache.ignite.network.processor.internal;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import org.jetbrains.annotations.Nullable;

/**
 * Various utilities for working with {@link TypeMirror} instances.
 */
class TypeUtils {
    /** */
    private final ProcessingEnvironment processingEnvironment;

    /** */
    TypeUtils(ProcessingEnvironment processingEnvironment) {
        this.processingEnvironment = processingEnvironment;
    }

    /**
     * Returns {@code true} if the <i>erasure</i> of the given types are actually the same type.
     */
    boolean isSameType(TypeMirror type1, Class<?> type2) {
        TypeMirror type2Mirror = typeMirrorFromClass(type2);

        return processingEnvironment.getTypeUtils().isSameType(erasure(type1), erasure(type2Mirror));
    }

    /**
     * Returns the primitive type represented by its boxed value or {@code null} if the given type is not a boxed
     * primitive type.
     */
    @Nullable
    PrimitiveType unboxedType(TypeMirror type) {
        try {
            return processingEnvironment.getTypeUtils().unboxedType(type);
        }
        catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    /**
     * Shortcut for the {@link Types#erasure(TypeMirror)} method.
     */
    private TypeMirror erasure(TypeMirror type) {
        return processingEnvironment.getTypeUtils().erasure(type);
    }

    /**
     * Creates a {@link TypeMirror} represented by the given {@link Class}.
     */
    private TypeMirror typeMirrorFromClass(Class<?> cls) {
        return processingEnvironment
            .getElementUtils()
            .getTypeElement(cls.getCanonicalName())
            .asType();
    }
}
