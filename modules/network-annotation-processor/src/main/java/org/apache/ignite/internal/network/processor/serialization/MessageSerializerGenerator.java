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

import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.network.serialization.MessageMappingException;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.apache.ignite.network.serialization.MessageWriter;

/**
 * Class for generating {@link MessageSerializer} classes.
 */
public class MessageSerializerGenerator {
    /** */
    private final ProcessingEnvironment processingEnvironment;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    /**
     * @param processingEnvironment processing environment
     * @param messageGroup message group
     */
    public MessageSerializerGenerator(ProcessingEnvironment processingEnvironment, MessageGroupWrapper messageGroup) {
        this.processingEnvironment = processingEnvironment;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a {@link MessageSerializer} class for the given network message type.
     *
     * @param message network message
     * @return {@code TypeSpec} of the generated serializer
     */
    public TypeSpec generateSerializer(MessageClass message) {
        processingEnvironment.getMessager()
            .printMessage(Diagnostic.Kind.NOTE, "Generating a MessageSerializer", message.element());

        return TypeSpec.classBuilder(message.simpleName() + "Serializer")
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageSerializer.class), message.className()))
            .addMethod(writeMessageMethod(message))
            .addOriginatingElement(message.element())
            .addOriginatingElement(messageGroup.element())
            .build();
    }

    /**
     * Generates the {@link MessageSerializer#writeMessage(NetworkMessage, MessageWriter)} implementation.
     */
    private MethodSpec writeMessageMethod(MessageClass message) {
        MethodSpec.Builder method = MethodSpec.methodBuilder("writeMessage")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(boolean.class)
            .addParameter(message.className(), "message")
            .addParameter(MessageWriter.class, "writer")
            .addException(MessageMappingException.class);

        List<ExecutableElement> getters = message.getters();

        method
            .beginControlFlow("if (!writer.isHeaderWritten())")
            .beginControlFlow(
                "if (!writer.writeHeader(message.groupType(), message.messageType(), (byte) $L))", getters.size()
            )
            .addStatement("return false")
            .endControlFlow()
            .addStatement("writer.onHeaderWritten()")
            .endControlFlow()
            .addCode("\n");

        method.beginControlFlow("switch (writer.state())");

        for (int i = 0; i < getters.size(); ++i) {
            method
                .beginControlFlow("case $L:", i)
                .addCode(writeMessageCodeBlock(getters.get(i)))
                .addCode("\n")
                .addStatement("writer.incrementState()")
                .endControlFlow()
                .addComment("Falls through");
        }

        method.endControlFlow();

        method.addCode("\n").addStatement("return true");

        return method.build();
    }

    /**
     * Helper method for resolving a {@link MessageWriter} "write*" call based on the message field type.
     */
    private CodeBlock writeMessageCodeBlock(ExecutableElement getter) {
        var methodResolver = new MessageWriterMethodResolver(processingEnvironment);

        CodeBlock writerMethodCall = CodeBlock.builder()
            .add("boolean written = writer.")
            .add(methodResolver.resolveWriteMethod(getter))
            .build();

        return CodeBlock.builder()
            .addStatement(writerMethodCall)
            .add("\n")
            .beginControlFlow("if (!written)")
            .addStatement("return false")
            .endControlFlow()
            .build();
    }
}
