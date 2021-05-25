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
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.processor.annotations.AutoSerializable;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageMappingException;
import org.apache.ignite.network.serialization.MessageReader;

/**
 * Class for generating {@link MessageDeserializer} classes.
 */
class MessageDeserializerGenerator {
    /**
     * Element representing a network message type declaration
     */
    private final TypeElement messageClass;

    /**
     * {@link ClassName} for the corresponding message type
     */
    private final ClassName messageClassName;

    /** */
    private final MessageReaderMethodResolver methodResolver;

    /**
     * @see #msgField()
     */
    private final FieldSpec msgField;

    /** */
    MessageDeserializerGenerator(ProcessingEnvironment processingEnvironment, TypeElement messageClass) {
        this.messageClass = messageClass;
        messageClassName = ClassName.get(messageClass);
        methodResolver = new MessageReaderMethodResolver(processingEnvironment);
        msgField = msgField();
    }

    /**
     * Generates a {@link MessageDeserializer} class for the given network message type.
     */
    TypeSpec generateDeserializer() {
        return TypeSpec.classBuilder(messageClassName.simpleName() + "Deserializer")
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageDeserializer.class), messageClassName))
            .addField(msgField)
            .addMethod(readMessageMethod())
            .addMethod(klassMethod())
            .addMethod(getMessageMethod())
            .build();
    }

    /**
     * Generates the declaration of a message builder field used to accumulate incoming message parts inside the
     * deserializer.
     */
    private FieldSpec msgField() {
        var builderClassName = ClassName.get(messageClassName.packageName(), messageClassName.simpleName(), "Builder");

        char firstChar = messageClassName.simpleName().charAt(0);
        String builderMethod = Character.toLowerCase(firstChar) + messageClassName.simpleName().substring(1);

        return FieldSpec.builder(builderClassName, "msg", Modifier.PRIVATE, Modifier.FINAL)
            .initializer("$T.$L()", getMessageFactoryType(), builderMethod)
            .build();
    }

    /**
     * Returns the {@link TypeName} of the message factory provided as the {@link AutoSerializable} annotation
     * parameter.
     *
     * @implNote This method uses a trick for obtaining a {@link TypeMirror} from a class that is declared inside the
     * source code (javac does not allow loading such classes).
     */
    private TypeName getMessageFactoryType() {
        try {
            // always throws
            messageClass.getAnnotation(AutoSerializable.class).messageFactory();
        }
        catch (MirroredTypeException e) {
            return TypeName.get(e.getTypeMirror());
        }

        throw new IllegalStateException("No MirroredTypeException thrown when trying to access the message factory");
    }

    /**
     * Generates the {@link MessageDeserializer#readMessage(MessageReader)} implementation.
     */
    private MethodSpec readMessageMethod() {
        MethodSpec.Builder method = MethodSpec.methodBuilder("readMessage")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(boolean.class)
            .addParameter(MessageReader.class, "reader")
            .addException(MessageMappingException.class);

        // find the Builder interface inside the message declaration
        Element builderInterface = messageClass.getEnclosedElements().stream()
            .filter(element -> element.getKind() == ElementKind.INTERFACE)
            .filter(element -> element.getSimpleName().contentEquals("Builder"))
            .findAny()
            .orElseThrow(() -> new ProcessingException("No nested Builder interface found"));

        // retrieve all methods, except the "build" method and treat them as setters
        List<ExecutableElement> setters = builderInterface.getEnclosedElements().stream()
            .filter(element -> element.getKind() == ElementKind.METHOD)
            .filter(element -> !element.getSimpleName().contentEquals("build"))
            .sorted(Comparator.comparing(element -> element.getSimpleName().toString()))
            .map(ExecutableElement.class::cast)
            .collect(Collectors.toList());

        method
            .beginControlFlow("if (!reader.beforeMessageRead())")
            .addStatement("return false")
            .endControlFlow()
            .addCode("\n");

        method.beginControlFlow("switch (reader.state())");

        for (int i = 0; i < setters.size(); ++i) {
            method
                .beginControlFlow("case $L:", i)
                .addStatement(readMessageCodeBlock(setters.get(i)))
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

        method.addCode("\n").addStatement("return reader.afterMessageRead($T.class)", messageClassName);

        return method.build();
    }

    /**
     * Helper method for resolving a {@link MessageReader} "read*" call based on the message field type.
     */
    private CodeBlock readMessageCodeBlock(ExecutableElement setter) {
        return CodeBlock.builder()
            .add("$N.$N(reader.", msgField, setter.getSimpleName())
            .add(methodResolver.resolveReadMethod(setter))
            .add(")")
            .build();
    }

    /**
     * Generates the {@link MessageDeserializer#klass()} implementation.
     */
    private MethodSpec klassMethod() {
        return MethodSpec.methodBuilder("klass")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Class.class), messageClassName))
            .addStatement("return $T.class", messageClassName)
            .build();
    }

    /**
     * Generates the {@link MessageDeserializer#getMessage()} implementation.
     */
    private MethodSpec getMessageMethod() {
        return MethodSpec.methodBuilder("getMessage")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(messageClassName)
            .addStatement("return $N.build()", msgField)
            .build();
    }
}
