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

import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import com.squareup.javapoet.CodeBlock;
import org.apache.ignite.network.serialization.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Class for resolving {@link MessageReader} "read*" methods for the corresponding message field type.
 */
class MessageReaderMethodResolver {
    /** */
    private final BaseMethodNameResolver methodNameResolver;

    /** */
    private final MessageCollectionItemTypeConverter typeConverter;

    /** */
    MessageReaderMethodResolver(ProcessingEnvironment processingEnvironment) {
        methodNameResolver = new BaseMethodNameResolver(processingEnvironment);
        typeConverter = new MessageCollectionItemTypeConverter(processingEnvironment);
    }

    /**
     * Resolves the "read" method by the type of the given message's builder method.
     */
    CodeBlock resolveReadMethod(ExecutableElement builderSetter) {
        if (builderSetter.getParameters().size() != 1) {
            throw new ProcessingException("Invalid number of parameters of a Builder setter (expected 1): " + builderSetter);
        }

        TypeMirror parameterType = builderSetter.getParameters().get(0).asType();

        String parameterName = builderSetter.getSimpleName().toString();

        String methodName = methodNameResolver.resolveBaseMethodName(parameterType);

        switch (methodName) {
            case "ObjectArray":
                return resolveReadObjectArray((ArrayType)parameterType, parameterName);
            case "Collection":
                return resolveReadCollection((DeclaredType)parameterType, parameterName);
            case "Map":
                return resolveReadMap((DeclaredType)parameterType, parameterName);
            default:
                return CodeBlock.builder().add("read$L($S)", methodName, parameterName).build();
        }
    }

    /**
     * Creates a {@link MessageReader#readObjectArray(String, MessageCollectionItemType, Class)} method call.
     */
    private CodeBlock resolveReadObjectArray(ArrayType parameterType, String parameterName) {
        TypeMirror componentType = parameterType.getComponentType();

        return CodeBlock.builder()
            .add(
                "readObjectArray($S, $T.$L, $T.class)",
                parameterName,
                MessageCollectionItemType.class,
                typeConverter.fromTypeMirror(componentType),
                componentType
            )
            .build();
    }

    /**
     * Creates a {@link MessageReader#readCollection(String, MessageCollectionItemType)} method call.
     */
    private CodeBlock resolveReadCollection(DeclaredType parameterType, String parameterName) {
        TypeMirror collectionGenericType = parameterType.getTypeArguments().get(0);

        return CodeBlock.builder()
            .add(
                "readCollection($S, $T.$L)",
                parameterName,
                MessageCollectionItemType.class,
                typeConverter.fromTypeMirror(collectionGenericType)
            )
            .build();
    }

    /**
     * Creates a {@link MessageReader#readMap(String, MessageCollectionItemType, MessageCollectionItemType, boolean)}
     * method call.
     */
    private CodeBlock resolveReadMap(DeclaredType parameterType, String parameterName) {
        List<? extends TypeMirror> typeArguments = parameterType.getTypeArguments();

        MessageCollectionItemType mapKeyType = typeConverter.fromTypeMirror(typeArguments.get(0));
        MessageCollectionItemType mapValueType = typeConverter.fromTypeMirror(typeArguments.get(1));

        return CodeBlock.builder()
            .add(
                "readMap($S, $T.$L, $T.$L, false)",
                parameterName,
                MessageCollectionItemType.class,
                mapKeyType,
                MessageCollectionItemType.class,
                mapValueType
            )
            .build();
    }
}
