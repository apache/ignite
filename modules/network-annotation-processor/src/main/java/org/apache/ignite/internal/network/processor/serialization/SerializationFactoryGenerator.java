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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Class for generating {@link MessageSerializationFactory} classes.
 */
public class SerializationFactoryGenerator {
    /**
     *
     */
    private final ProcessingEnvironment processingEnv;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    /**
     * @param processingEnv processing environment
     * @param messageGroup  message group
     */
    public SerializationFactoryGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a {@link MessageSerializationFactory} class for the given network message type.
     *
     * @param message      network message
     * @param serializer   {@link MessageSerializer}, generated for the given {@code message}
     * @param deserializer {@link MessageDeserializer}, generated for the given {@code message}
     * @return {@code TypeSpec} of the generated {@code MessageSerializationFactory}
     */
    public TypeSpec generateFactory(MessageClass message, TypeSpec serializer, TypeSpec deserializer) {
        processingEnv.getMessager()
                .printMessage(Diagnostic.Kind.NOTE, "Generating a MessageSerializationFactory", message.element());

        ClassName messageFactoryClassName = messageGroup.messageFactoryClassName();

        FieldSpec messageFactoryField = FieldSpec.builder(messageFactoryClassName, "messageFactory")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build();

        return TypeSpec.classBuilder(message.simpleName() + "SerializationFactory")
                .addModifiers(Modifier.PUBLIC)
                .addSuperinterface(
                        ParameterizedTypeName.get(ClassName.get(MessageSerializationFactory.class), message.className())
                )
                .addField(messageFactoryField)
                .addMethod(
                        MethodSpec.constructorBuilder()
                                .addModifiers(Modifier.PUBLIC)
                                .addParameter(messageFactoryClassName, "messageFactory")
                                .addStatement("this.$N = messageFactory", messageFactoryField)
                                .build()
                )
                .addMethod(
                        MethodSpec.methodBuilder("createDeserializer")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC)
                                .returns(ParameterizedTypeName.get(ClassName.get(MessageDeserializer.class), message.className()))
                                .addStatement("return new $N($N)", deserializer, messageFactoryField)
                                .build()
                )
                .addMethod(
                        MethodSpec.methodBuilder("createSerializer")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC)
                                .returns(ParameterizedTypeName.get(ClassName.get(MessageSerializer.class), message.className()))
                                .addStatement("return new $N()", serializer)
                                .build()
                )
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element())
                .build();
    }
}
