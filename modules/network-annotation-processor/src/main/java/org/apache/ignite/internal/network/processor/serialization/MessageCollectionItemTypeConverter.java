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
import java.util.UUID;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.network.processor.ProcessingException;
import org.apache.ignite.internal.network.processor.TypeUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Class for extracting {@link MessageCollectionItemType} from different type representations.
 */
class MessageCollectionItemTypeConverter {
    /** */
    private final ProcessingEnvironment processingEnvironment;

    /**
     * @param processingEnvironment processing environment
     */
    MessageCollectionItemTypeConverter(ProcessingEnvironment processingEnvironment) {
        this.processingEnvironment = processingEnvironment;
    }

    /**
     * Converts the given {@link ArrayType} into a {@link MessageCollectionItemType}.
     */
    private static MessageCollectionItemType fromArrayType(ArrayType parameterType) {
        switch (parameterType.getComponentType().getKind()) {
            case BYTE:
                return MessageCollectionItemType.BYTE_ARR;
            case SHORT:
                return MessageCollectionItemType.SHORT_ARR;
            case CHAR:
                return MessageCollectionItemType.CHAR_ARR;
            case INT:
                return MessageCollectionItemType.INT_ARR;
            case LONG:
                return MessageCollectionItemType.LONG_ARR;
            case FLOAT:
                return MessageCollectionItemType.FLOAT_ARR;
            case DOUBLE:
                return MessageCollectionItemType.DOUBLE_ARR;
            case BOOLEAN:
                return MessageCollectionItemType.BOOLEAN_ARR;
            default:
                throw new ProcessingException("Unsupported MessageCollectionItemType: " + parameterType);
        }
    }

    /**
     * Converts the given {@link TypeMirror} into a {@link MessageCollectionItemType}.
     *
     * @param parameterType type mirror
     * @return corresponding {@code MessageCollectionItemType}
     */
    MessageCollectionItemType fromTypeMirror(TypeMirror parameterType) {
        switch (parameterType.getKind()) {
            case BYTE:
                return MessageCollectionItemType.BYTE;
            case SHORT:
                return MessageCollectionItemType.SHORT;
            case CHAR:
                return MessageCollectionItemType.CHAR;
            case INT:
                return MessageCollectionItemType.INT;
            case LONG:
                return MessageCollectionItemType.LONG;
            case FLOAT:
                return MessageCollectionItemType.FLOAT;
            case DOUBLE:
                return MessageCollectionItemType.DOUBLE;
            case BOOLEAN:
                return MessageCollectionItemType.BOOLEAN;
            case ARRAY:
                return fromArrayType((ArrayType)parameterType);
            case DECLARED:
                return fromDeclaredType((DeclaredType)parameterType);
            default:
                throw new ProcessingException("Unsupported MessageCollectionItemType: " + parameterType);
        }
    }

    /**
     * Converts the given {@link DeclaredType} into a {@link MessageCollectionItemType}.
     */
    private MessageCollectionItemType fromDeclaredType(DeclaredType parameterType) {
        var typeUtils = new TypeUtils(processingEnvironment);

        PrimitiveType unboxedType = typeUtils.unboxedType(parameterType);

        if (unboxedType != null)
            return fromTypeMirror(unboxedType);
        else if (typeUtils.isSameType(parameterType, String.class))
            return MessageCollectionItemType.STRING;
        else if (typeUtils.isSameType(parameterType, UUID.class))
            return MessageCollectionItemType.UUID;
        else if (typeUtils.isSameType(parameterType, IgniteUuid.class))
            return MessageCollectionItemType.IGNITE_UUID;
        else if (typeUtils.isSameType(parameterType, NetworkMessage.class))
            return MessageCollectionItemType.MSG;
        else if (typeUtils.isSameType(parameterType, BitSet.class))
            return MessageCollectionItemType.BIT_SET;
        else
            throw new ProcessingException("Unsupported MessageCollectionItemType: " + parameterType);
    }
}
