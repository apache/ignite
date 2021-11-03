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

package org.apache.ignite.internal.network.processor.serialization;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.network.processor.ProcessingException;
import org.apache.ignite.internal.network.processor.TypeUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;

/**
 * Class for resolving a "base" part of a (de-)serialization method based on the message type. This part is then used by concrete method
 * resolvers by prepending a "read"/"write" prefix and adding call arguments.
 *
 * @see MessageReaderMethodResolver
 * @see MessageWriterMethodResolver
 */
class BaseMethodNameResolver {
    /**
     *
     */
    private final ProcessingEnvironment processingEnvironment;

    /**
     * @param processingEnvironment processing environment
     */
    BaseMethodNameResolver(ProcessingEnvironment processingEnvironment) {
        this.processingEnvironment = processingEnvironment;
    }

    /**
     * Resolves a "base" part of a (de-)serialization method.
     *
     * @param parameterType parameter of the method to resolve
     * @return part of the method name, depending on the parameter type
     */
    String resolveBaseMethodName(TypeMirror parameterType) {
        if (parameterType.getKind().isPrimitive()) {
            return resolvePrimitiveMethodName(parameterType);
        } else if (parameterType.getKind() == TypeKind.ARRAY) {
            return resolveArrayMethodName((ArrayType) parameterType);
        } else if (parameterType.getKind() == TypeKind.DECLARED) {
            return resolveReferenceMethodName((DeclaredType) parameterType);
        } else {
            throw new ProcessingException("Unsupported type for message (de-)serialization: " + parameterType);
        }
    }

    /**
     * Resolves a "base" part of a (de-)serialization method for the given primitive type.
     */
    private static String resolvePrimitiveMethodName(TypeMirror parameterType) {
        switch (parameterType.getKind()) {
            case BYTE:
                return "Byte";
            case SHORT:
                return "Short";
            case CHAR:
                return "Char";
            case INT:
                return "Int";
            case LONG:
                return "Long";
            case FLOAT:
                return "Float";
            case DOUBLE:
                return "Double";
            case BOOLEAN:
                return "Boolean";
            default:
                throw new ProcessingException(String.format("Parameter type %s is not primitive", parameterType));
        }
    }

    /**
     * Resolves a "base" part of a (de-)serialization method for the given array.
     */
    private static String resolveArrayMethodName(ArrayType parameterType) {
        if (parameterType.getComponentType().getKind().isPrimitive()) {
            return resolvePrimitiveMethodName(parameterType.getComponentType()) + "Array";
        } else {
            return "ObjectArray";
        }
    }

    /**
     * Resolves a "base" part of a (de-)serialization method for the given reference type.
     */
    private String resolveReferenceMethodName(DeclaredType parameterType) {
        var typeUtils = new TypeUtils(processingEnvironment);

        if (typeUtils.isSameType(parameterType, String.class)) {
            return "String";
        } else if (typeUtils.isSameType(parameterType, UUID.class)) {
            return "Uuid";
        } else if (typeUtils.isSameType(parameterType, IgniteUuid.class)) {
            return "IgniteUuid";
        } else if (typeUtils.isSameType(parameterType, NetworkMessage.class)) {
            return "Message";
        } else if (typeUtils.isSameType(parameterType, BitSet.class)) {
            return "BitSet";
        } else if (typeUtils.isSameType(parameterType, Collection.class)) {
            return "Collection";
        } else if (typeUtils.isSameType(parameterType, Map.class)) {
            return "Map";
        } else {
            throw new ProcessingException("Unsupported reference type for message (de-)serialization: " + parameterType);
        }
    }
}
