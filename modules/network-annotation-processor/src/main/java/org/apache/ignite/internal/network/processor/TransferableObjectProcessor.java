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

import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.processor.messages.MessageBuilderGenerator;
import org.apache.ignite.internal.network.processor.messages.MessageFactoryGenerator;
import org.apache.ignite.internal.network.processor.messages.MessageImplGenerator;
import org.apache.ignite.internal.network.processor.serialization.MessageDeserializerGenerator;
import org.apache.ignite.internal.network.processor.serialization.MessageSerializerGenerator;
import org.apache.ignite.internal.network.processor.serialization.RegistryInitializerGenerator;
import org.apache.ignite.internal.network.processor.serialization.SerializationFactoryGenerator;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.MessageGroup;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Annotation processor for working with the {@link Transferable} annotation.
 */
public class TransferableObjectProcessor extends AbstractProcessor {
    /** {@inheritDoc} */
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(Transferable.class.getName());
    }

    /** {@inheritDoc} */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /** {@inheritDoc} */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            List<MessageClass> messages = annotations.stream()
                    .map(roundEnv::getElementsAnnotatedWith)
                    .flatMap(Collection::stream)
                    .map(TypeElement.class::cast)
                    .map(e -> new MessageClass(processingEnv, e))
                    .collect(Collectors.toList());

            if (messages.isEmpty()) {
                return true;
            }

            MessageGroupWrapper messageGroup = getMessageGroup(roundEnv);

            validateMessages(messages);

            generateMessageImpls(messages, messageGroup);

            generateSerializers(messages, messageGroup);
        } catch (ProcessingException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), e.getElement());
        }

        return true;
    }

    /**
     * Generates the following classes for the current compilation unit:
     *
     * <ol>
     *     <li>Builder interfaces;</li>
     *     <li>Network Message and Builder implementations;</li>
     *     <li>Message factory for all generated messages.</li>
     * </ol>
     */
    private void generateMessageImpls(List<MessageClass> annotatedMessages, MessageGroupWrapper messageGroup) {
        var messageBuilderGenerator = new MessageBuilderGenerator(processingEnv, messageGroup);
        var messageImplGenerator = new MessageImplGenerator(processingEnv, messageGroup);
        var messageFactoryGenerator = new MessageFactoryGenerator(processingEnv, messageGroup);

        for (MessageClass message : annotatedMessages) {
            try {
                // generate a Builder interface with setters
                TypeSpec builder = messageBuilderGenerator.generateBuilderInterface(message);

                writeToFile(message.packageName(), builder);

                // generate the message and the builder implementations
                TypeSpec messageImpl = messageImplGenerator.generateMessageImpl(message, builder);

                writeToFile(message.packageName(), messageImpl);
            } catch (ProcessingException e) {
                throw new ProcessingException(e.getMessage(), e.getCause(), message.element());
            }
        }

        // generate a factory for all messages inside the current compilation unit
        TypeSpec messageFactory = messageFactoryGenerator.generateMessageFactory(annotatedMessages);

        writeToFile(messageGroup.packageName(), messageFactory);
    }

    /**
     * Generates the following classes for the current compilation unit:
     *
     * <ol>
     *     <li>{@link MessageSerializer};</li>
     *     <li>{@link MessageDeserializer};</li>
     *     <li>{@link MessageSerializationFactory};</li>
     *     <li>Helper class for adding all generated serialization factories to a
     *     {@link MessageSerializationRegistry}.</li>
     * </ol>
     */
    private void generateSerializers(List<MessageClass> annotatedMessages, MessageGroupWrapper messageGroup) {
        List<MessageClass> serializableMessages = annotatedMessages.stream()
                .filter(MessageClass::isAutoSerializable)
                .collect(Collectors.toList());

        if (serializableMessages.isEmpty()) {
            return;
        }

        var factories = new HashMap<MessageClass, TypeSpec>();

        var serializerGenerator = new MessageSerializerGenerator(processingEnv, messageGroup);
        var deserializerGenerator = new MessageDeserializerGenerator(processingEnv, messageGroup);
        var factoryGenerator = new SerializationFactoryGenerator(processingEnv, messageGroup);
        var initializerGenerator = new RegistryInitializerGenerator(processingEnv, messageGroup);

        for (MessageClass message : serializableMessages) {
            try {
                // MessageSerializer
                TypeSpec serializer = serializerGenerator.generateSerializer(message);

                writeToFile(message.packageName(), serializer);

                // MessageDeserializer
                TypeSpec deserializer = deserializerGenerator.generateDeserializer(message);

                writeToFile(message.packageName(), deserializer);

                // MessageSerializationFactory
                TypeSpec factory = factoryGenerator.generateFactory(message, serializer, deserializer);

                writeToFile(message.packageName(), factory);

                factories.put(message, factory);
            } catch (ProcessingException e) {
                throw new ProcessingException(e.getMessage(), e.getCause(), message.element());
            }
        }

        TypeSpec registryInitializer = initializerGenerator.generateRegistryInitializer(factories);

        writeToFile(messageGroup.packageName(), registryInitializer);
    }

    /**
     * Validates the annotated messages:
     *
     * <ol>
     *     <li>{@link Transferable} annotation is present on a valid element;</li>
     *     <li>No messages with the same message type exist.</li>
     * </ol>
     */
    private void validateMessages(List<MessageClass> messages) {
        var typeUtils = new TypeUtils(processingEnv);

        var messageTypesSet = new HashSet<Short>();

        for (MessageClass message : messages) {
            TypeElement element = message.element();

            boolean isValid = element.getKind() == ElementKind.INTERFACE
                    && typeUtils.hasSuperInterface(element, NetworkMessage.class);

            if (!isValid) {
                var errorMsg = String.format(
                        "%s annotation must only be present on interfaces that extend %s",
                        Transferable.class, NetworkMessage.class
                );

                throw new ProcessingException(errorMsg, null, element);
            }

            short messageType = message.messageType();

            if (!messageTypesSet.add(messageType)) {
                var errorMsg = String.format(
                        "Conflicting message types in a group, message with type %d already exists",
                        messageType
                );

                throw new ProcessingException(errorMsg, null, element);
            }
        }
    }

    /**
     * Extracts and validates the declared message group types marked with the {@link MessageGroup} annotation.
     */
    private static MessageGroupWrapper getMessageGroup(RoundEnvironment roundEnv) {
        Set<? extends Element> messageGroupSet = roundEnv.getElementsAnnotatedWith(MessageGroup.class);

        if (messageGroupSet.isEmpty()) {
            throw new ProcessingException("No message groups (classes annotated with @MessageGroup) found");
        }

        if (messageGroupSet.size() != 1) {
            List<String> sortedNames = messageGroupSet.stream()
                    .map(Object::toString)
                    .sorted()
                    .collect(Collectors.toList());

            throw new ProcessingException(
                    "Invalid number of message groups (classes annotated with @MessageGroup), "
                            + "only one can be present in a compilation unit: " + sortedNames
            );
        }

        Element singleElement = messageGroupSet.iterator().next();

        return new MessageGroupWrapper((TypeElement) singleElement);
    }

    /**
     * Writes the given generated class into a file.
     */
    private void writeToFile(String packageName, TypeSpec typeSpec) {
        try {
            JavaFile
                    .builder(packageName, typeSpec)
                    .indent(" ".repeat(4))
                    .build()
                    .writeTo(processingEnv.getFiler());
        } catch (IOException e) {
            throw new ProcessingException("IO exception during annotation processing", e);
        }
    }
}
