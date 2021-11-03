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
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageMappingException;
import org.apache.ignite.network.serialization.MessageReader;

/**
 * Class for generating {@link MessageDeserializer} classes.
 */
public class MessageDeserializerGenerator {
    /**
     *
     */
    private final ProcessingEnvironment processingEnv;

    /**
     * Message Types declarations for the current module.
     */
    private final MessageGroupWrapper messageGroup;

    /**
     * @param processingEnv processing environment
     * @param messageGroup  message group
     */
    public MessageDeserializerGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a {@link MessageDeserializer} class for the given network message type.
     *
     * @param message network message
     * @return {@code TypeSpec} of the generated deserializer
     */
    public TypeSpec generateDeserializer(MessageClass message) {
        processingEnv.getMessager()
                .printMessage(Diagnostic.Kind.NOTE, "Generating a MessageDeserializer", message.element());

        FieldSpec msgField = FieldSpec.builder(message.builderClassName(), "msg")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build();

        return TypeSpec.classBuilder(message.simpleName() + "Deserializer")
                .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageDeserializer.class), message.className()))
                .addField(msgField)
                .addMethod(
                        MethodSpec.constructorBuilder()
                                .addParameter(messageGroup.messageFactoryClassName(), "messageFactory")
                                .addStatement("this.$N = messageFactory.$L()", msgField, message.asMethodName())
                                .build()
                )
                .addMethod(
                        MethodSpec.methodBuilder("klass")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC)
                                .returns(ParameterizedTypeName.get(ClassName.get(Class.class), message.className()))
                                .addStatement("return $T.class", message.className())
                                .build()
                )
                .addMethod(
                        MethodSpec.methodBuilder("getMessage")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC)
                                .returns(message.className())
                                .addStatement("return $N.build()", msgField)
                                .build()
                )
                .addMethod(readMessageMethod(message, msgField))
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element())
                .build();
    }

    /**
     * Generates the {@link MessageDeserializer#readMessage(MessageReader)} implementation.
     */
    private MethodSpec readMessageMethod(MessageClass message, FieldSpec msgField) {
        MethodSpec.Builder method = MethodSpec.methodBuilder("readMessage")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addParameter(MessageReader.class, "reader")
                .addException(MessageMappingException.class);

        List<ExecutableElement> getters = message.getters();

        method
                .beginControlFlow("if (!reader.beforeMessageRead())")
                .addStatement("return false")
                .endControlFlow()
                .addCode("\n");

        method.beginControlFlow("switch (reader.state())");

        for (int i = 0; i < getters.size(); ++i) {
            method
                    .beginControlFlow("case $L:", i)
                    .addStatement(readMessageCodeBlock(getters.get(i), msgField))
                    .addCode("\n")
                    .addCode(CodeBlock.builder()
                            .beginControlFlow("if (!reader.isLastRead())")
                            .addStatement("return false")
                            .endControlFlow()
                            .build()
                    )
                    .addCode("\n")
                    .addStatement("reader.incrementState()")
                    .endControlFlow()
                    .addComment("Falls through");
        }

        method.endControlFlow();

        method.addCode("\n").addStatement("return reader.afterMessageRead($T.class)", message.className());

        return method.build();
    }

    /**
     * Helper method for resolving a {@link MessageReader} "read*" call based on the message field type.
     */
    private CodeBlock readMessageCodeBlock(ExecutableElement getter, FieldSpec msgField) {
        var methodResolver = new MessageReaderMethodResolver(processingEnv);

        return CodeBlock.builder()
                .add("$N.$N(reader.", msgField, getter.getSimpleName())
                .add(methodResolver.resolveReadMethod(getter))
                .add(")")
                .build();
    }
}
