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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageMappingException;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.apache.ignite.network.serialization.MessageWriter;

/**
 * Class for generating {@link MessageSerializer} classes.
 */
class MessageSerializerGenerator {
    /**
     * Element representing a network message type declaration
     */
    private final TypeElement messageClass;

    /**
     * {@link ClassName} for the corresponding message type
     */
    private final ClassName messageClassName;

    /** */
    private final MessageWriterMethodResolver methodResolver;

    /** */
    MessageSerializerGenerator(ProcessingEnvironment processingEnvironment, TypeElement messageClass) {
        this.messageClass = messageClass;
        messageClassName = ClassName.get(messageClass);
        methodResolver = new MessageWriterMethodResolver(processingEnvironment);
    }

    /**
     * Generates a {@link MessageSerializer} class for the given network message type.
     */
    TypeSpec generateSerializer() {
        return TypeSpec.classBuilder(messageClassName.simpleName() + "Serializer")
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageSerializer.class), messageClassName))
            .addMethod(writeMessageMethod())
            .build();
    }

    /**
     * Generates the {@link MessageSerializer#writeMessage(NetworkMessage, MessageWriter)} implementation.
     */
    private MethodSpec writeMessageMethod() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("writeMessage")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(boolean.class)
            .addParameter(messageClassName, "message")
            .addParameter(MessageWriter.class, "writer")
            .addException(MessageMappingException.class);

        List<ExecutableElement> getters = messageClass.getEnclosedElements().stream()
            .filter(element -> element.getKind() == ElementKind.METHOD)
            .filter(element -> !element.getSimpleName().contentEquals("directType"))
            .sorted(Comparator.comparing(element -> element.getSimpleName().toString()))
            .map(ExecutableElement.class::cast)
            .collect(Collectors.toList());

        method
            .beginControlFlow("if (!writer.isHeaderWritten())")
            .beginControlFlow("if (!writer.writeHeader(message.directType(), (byte) $L))", getters.size())
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
