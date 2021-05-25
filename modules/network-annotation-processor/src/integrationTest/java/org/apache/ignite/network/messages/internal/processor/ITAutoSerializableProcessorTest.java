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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.tools.JavaFileObject;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.apache.ignite.network.NetworkMessage;
import org.junit.jupiter.api.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;

/**
 * Integration tests for {@link AutoSerializableProcessor}.
 */
public class ITAutoSerializableProcessorTest {
    /**
     * Package name of the test sources.
     */
    private static final String RESOURCE_PACKAGE_NAME = "org.apache.ignite.network.processor.internal.";

    /**
     * Compiler instance configured with the annotation processor being tested.
     */
    private final Compiler compiler = Compiler.javac().withProcessors(new AutoSerializableProcessor());

    /**
     * Compiles the network message with all supported directly marshallable types and checks that the compilation
     * completed successfully.
     */
    @Test
    void testCompileAllTypesMessage() {
        Compilation compilation = compiler.compile(
            getSources("AllTypesMessage", "AllTypesMessageImpl", "AllTypesMessageFactory")
        );

        assertThat(compilation).succeededWithoutWarnings();

        assertThat(compilation).generatedSourceFile(RESOURCE_PACKAGE_NAME + "AllTypesMessageSerializer");
        assertThat(compilation).generatedSourceFile(RESOURCE_PACKAGE_NAME + "AllTypesMessageDeserializer");
        assertThat(compilation).generatedSourceFile(RESOURCE_PACKAGE_NAME + "AllTypesMessageSerializationFactory");
    }

    /**
     * Compiles a test message that doesn't extend {@link NetworkMessage}.
     */
    @Test
    void testInvalidAnnotatedTypeMessage() {
        Compilation compilation = compiler.compile(
            getSources("InvalidAnnotatedTypeMessage", "AllTypesMessageImpl", "AllTypesMessageFactory")
        );

        assertThat(compilation).hadErrorContaining("annotation must only be present on interfaces that extend");
    }

    /**
     * Compiles a test message that contains an unsupported content type.
     */
    @Test
    void testUnsupportedTypeMessage() {
        Compilation compilation = compiler.compile(
            getSources("UnsupportedTypeMessage", "AllTypesMessageImpl", "AllTypesMessageFactory")
        );

        assertThat(compilation).hadErrorContaining("Unsupported reference type for message (de-)serialization: java.util.ArrayList");
    }

    /**
     * Compiles a test message that violates the message contract by not declaring a {@code Builder} interface.
     */
    @Test
    void testMissingBuilderMessage() {
        Compilation compilation = compiler.compile(
            getSources("MissingBuilderMessage", "AllTypesMessageImpl", "AllTypesMessageFactory")
        );

        assertThat(compilation).hadErrorContaining("No nested Builder interface found");
    }

    /**
     * Compiles a test message that violates the message contract by declaring a getter with {@code void} return type.
     */
    @Test
    void testInvalidGetterMessage() {
        Compilation compilation = compiler.compile(
            getSources("InvalidGetterMessage", "AllTypesMessageImpl", "AllTypesMessageFactory")
        );

        assertThat(compilation).hadErrorContaining("Getter method a() does not return any value");
    }

    /**
     * Compiles a network message that does not implement {@link NetworkMessage} directly but rather through a bunch of
     * superinterfaces.
     */
    @Test
    void testTransitiveMessage() {
        Compilation compilation = compiler.compile(
            getSources("TransitiveMessage", "TransitiveMessageFactory")
        );

        assertThat(compilation).succeededWithoutWarnings();

        assertThat(compilation).generatedSourceFile(RESOURCE_PACKAGE_NAME + "TransitiveMessageSerializer");
        assertThat(compilation).generatedSourceFile(RESOURCE_PACKAGE_NAME + "TransitiveMessageDeserializer");
        assertThat(compilation).generatedSourceFile(RESOURCE_PACKAGE_NAME + "TransitiveMessageSerializationFactory");
    }

    /**
     * Converts given test source class names to a list of {@link JavaFileObject}s.
     */
    private static List<JavaFileObject> getSources(String... sourceNames) {
        return Arrays.stream(sourceNames)
            .map(source -> RESOURCE_PACKAGE_NAME.replace('.', '/') + source + ".java")
            .map(JavaFileObjects::forResource)
            .collect(Collectors.toList());
    }
}
