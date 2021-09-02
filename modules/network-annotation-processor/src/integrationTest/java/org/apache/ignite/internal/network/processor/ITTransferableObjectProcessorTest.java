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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.tools.JavaFileObject;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.network.annotations.MessageGroup;
import org.junit.jupiter.api.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for the {@link TransferableObjectProcessor}.
 */
public class ITTransferableObjectProcessorTest {
    /**
     * Package name of the test sources.
     */
    private static final String RESOURCE_PACKAGE_NAME = "org.apache.ignite.internal.network.processor.";

    /**
     * Compiler instance configured with the annotation processor being tested.
     */
    private final Compiler compiler = Compiler.javac().withProcessors(new TransferableObjectProcessor());

    /**
     * Compiles the network message with all supported directly marshallable types and checks that the compilation
     * completed successfully.
     */
    @Test
    void testCompileAllTypesMessage() {
        Compilation compilation = compile("AllTypesMessage");

        assertThat(compilation).succeededWithoutWarnings();

        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageBuilder"));
        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageImpl"));
        assertThat(compilation).generatedSourceFile(fileName("NetworkMessageProcessorTestFactory"));

        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageSerializer"));
        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageDeserializer"));
        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageSerializationFactory"));
        assertThat(compilation).generatedSourceFile(
            fileName("NetworkMessageProcessorTestSerializationRegistryInitializer")
        );
    }

    /**
     * Compiles a network message that does not implement {@link NetworkMessage} directly but rather through a bunch of
     * superinterfaces.
     */
    @Test
    void testTransitiveMessage() {
        Compilation compilation = compile("TransitiveMessage");

        assertThat(compilation).succeededWithoutWarnings();

        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageBuilder"));
        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageImpl"));
        assertThat(compilation).generatedSourceFile(fileName("NetworkMessageProcessorTestFactory"));

        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageSerializer"));
        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageDeserializer"));
        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageSerializationFactory"));
        assertThat(compilation).generatedSourceFile(
            fileName("NetworkMessageProcessorTestSerializationRegistryInitializer")
        );
    }

    /**
     * Tests that compilation of multiple well-formed messages is successful.
     */
    @Test
    void testCompileMultipleMessage() {
        Compilation compilation = compiler.compile(
            sources("AllTypesMessage", "TransitiveMessage", "ITTestMessageGroup")
        );

        assertThat(compilation).succeededWithoutWarnings();

        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageBuilder"));
        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageImpl"));

        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageBuilder"));
        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageImpl"));

        assertThat(compilation).generatedSourceFile(fileName("NetworkMessageProcessorTestFactory"));

        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageSerializer"));
        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageDeserializer"));
        assertThat(compilation).generatedSourceFile(fileName("AllTypesMessageSerializationFactory"));

        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageSerializer"));
        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageDeserializer"));
        assertThat(compilation).generatedSourceFile(fileName("TransitiveMessageSerializationFactory"));

        assertThat(compilation).generatedSourceFile(
            fileName("NetworkMessageProcessorTestSerializationRegistryInitializer")
        );
    }

    /**
     * Compiles a test message that doesn't extend {@link NetworkMessage}.
     */
    @Test
    void testInvalidAnnotatedTypeMessage() {
        Compilation compilation = compile("InvalidAnnotatedTypeMessage");

        assertThat(compilation).hadErrorContaining("annotation must only be present on interfaces that extend");
    }

    /**
     * Compiles a test message that contains an unsupported content type.
     */
    @Test
    void testUnmarshallableTypeMessage() {
        Compilation compilation = compile("UnmarshallableTypeMessage");

        assertThat(compilation).hadErrorContaining(
            "Unsupported reference type for message (de-)serialization: java.util.ArrayList"
        );
    }

    /**
     * Compiles a test message that violates the message contract by declaring a getter with {@code void} return type.
     */
    @Test
    void testInvalidReturnTypeGetterMessage() {
        Compilation compilation = compile("InvalidReturnTypeGetterMessage");

        assertThat(compilation).hadErrorContaining("Invalid getter method a()");
    }

    /**
     * Compiles a test message that violates the message contract by declaring a getter with a parameter.
     */
    @Test
    void testInvalidParameterGetterMessage() {
        Compilation compilation = compile("InvalidParameterGetterMessage");

        assertThat(compilation).hadErrorContaining("Invalid getter method a(int)");
    }

    /**
     * Tests that compilation fails if no {@link MessageGroup} annotated elements were found.
     */
    @Test
    void testMissingMessageGroup() {
        Compilation compilation = compiler.compile(sources("AllTypesMessage"));

        assertThat(compilation).hadErrorContaining(
            "No message groups (classes annotated with @MessageGroup) found"
        );
    }

    /**
     * Tests that compilation fails if multiple {@link MessageGroup} annotated elements were found.
     */
    @Test
    void testMultipleMessageGroups() {
        Compilation compilation = compiler.compile(
            sources("AllTypesMessage", "ConflictingTypeMessage", "ITTestMessageGroup", "SecondGroup")
        );

        assertThat(compilation).hadErrorContaining(
            "Invalid number of message groups (classes annotated with @MessageGroup), " +
                "only one can be present in a compilation unit: " +
                "[org.apache.ignite.internal.network.processor.ITTestMessageGroup, " +
                "org.apache.ignite.internal.network.processor.SecondGroup]"
        );
    }

    /**
     * Tests that setting the {@link Transferable#autoSerializable()} to {@code false} does not produce any
     * serialization-related classes and errors.
     */
    @Test
    void testNonSerializableMessage() {
        Compilation compilation = compile("UnmarshallableTypeNonSerializableMessage");

        assertThat(compilation).succeededWithoutWarnings();

        assertThat(compilation).generatedSourceFile(fileName("UnmarshallableTypeNonSerializableMessageBuilder"));
        assertThat(compilation).generatedSourceFile(fileName("UnmarshallableTypeNonSerializableMessageImpl"));
        assertThat(compilation).generatedSourceFile(fileName("NetworkMessageProcessorTestFactory"));

        // test that no additional classes have been generated
        assertThrows(
            AssertionError.class,
            () -> assertThat(compilation)
                .generatedSourceFile(fileName("UnmarshallableTypeNonSerializableMessageSerializer"))
        );
    }

    /**
     * Tests that messages with the same message type fail to compile.
     */
    @Test
    void testConflictingMessageTypes() {
        Compilation compilation = compiler.compile(
            sources("AllTypesMessage", "ConflictingTypeMessage", "ITTestMessageGroup")
        );

        assertThat(compilation).hadErrorContaining("message with type 1 already exists");
    }

    /**
     * Tests that if a message getter clashes with a getter in a superinterface, an appropriate error is displayed.
     */
    @Test
    void testInheritedMessageClash() {
        Compilation compilation = compile("InheritedMessageClash");

        assertThat(compilation).hadErrorContaining("Getter with name 'x' is already defined");
    }

    /**
     * Compiles the given network message.
     */
    private Compilation compile(String messageSource) {
        return compiler.compile(sources(messageSource, "ITTestMessageGroup"));
    }

    /**
     * Converts given test source class names to a list of {@link JavaFileObject}s.
     */
    private static List<JavaFileObject> sources(String... sources) {
        return Arrays.stream(sources)
            .map(source -> RESOURCE_PACKAGE_NAME.replace('.', '/') + source + ".java")
            .map(JavaFileObjects::forResource)
            .collect(Collectors.toList());
    }

    /** */
    private static String fileName(String className) {
        return RESOURCE_PACKAGE_NAME + className;
    }
}
