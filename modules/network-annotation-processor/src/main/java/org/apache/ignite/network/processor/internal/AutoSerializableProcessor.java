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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.processor.annotations.AutoSerializable;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Annotation processor for generating (de-)serializers for network messages marked with the {@link AutoSerializable}
 * annotation.
 */
public class AutoSerializableProcessor extends AbstractProcessor {
    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Set.of(AutoSerializable.class.getName());
    }

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<TypeElement> annotatedElements = annotations.stream()
            .map(roundEnv::getElementsAnnotatedWith)
            .flatMap(Collection::stream)
            .map(TypeElement.class::cast)
            .collect(Collectors.toUnmodifiableSet());

        if (annotatedElements.isEmpty()) {
            return true;
        }

        try {
            generateSources(annotatedElements);
        }
        catch (IOException e) {
            throw new IllegalStateException("IO exception during annotation processing", e);
        }

        return true;
    }

    /**
     * Generates serialization-related classes for the given elements.
     */
    private void generateSources(Set<TypeElement> annotatedElements) throws IOException {
        var factories = new HashMap<TypeElement, TypeSpec>();

        for (var messageClass : annotatedElements) {
            try {
                if (isValidElement(messageClass)) {
                    String packageName = ClassName.get(messageClass).packageName();

                    TypeSpec serializer = generateSerializer(messageClass);
                    writeToFile(packageName, serializer);

                    TypeSpec deserializer = generateDeseralizer(messageClass);
                    writeToFile(packageName, deserializer);

                    TypeSpec factory = generateFactory(messageClass, serializer, deserializer);
                    writeToFile(packageName, factory);

                    factories.put(messageClass, factory);
                }
                else {
                    throw new ProcessingException(String.format(
                        "%s annotation must only be present on interfaces that extend %s",
                        AutoSerializable.class, NetworkMessage.class
                    ));
                }
            }
            catch (ProcessingException e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), messageClass);
            }
        }

        TypeSpec registryInitializer = generateRegistryInitializer(factories);
        writeToFile(getParentPackage(annotatedElements), registryInitializer);
    }

    /**
     * Generates a {@link MessageSerializer}.
     */
    private TypeSpec generateSerializer(TypeElement messageClass) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating a MessageSerializer", messageClass);

        return new MessageSerializerGenerator(processingEnv, messageClass).generateSerializer();
    }

    /**
     * Generates a {@link MessageDeserializer}.
     */
    private TypeSpec generateDeseralizer(TypeElement messageClass) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating a MessageDeserializer", messageClass);

        return new MessageDeserializerGenerator(processingEnv, messageClass).generateDeserializer();
    }

    /**
     * Generates a {@link MessageSerializationFactory}.
     */
    private TypeSpec generateFactory(TypeElement messageClass, TypeSpec serializer, TypeSpec deserializer) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating a MessageSerializationFactory", messageClass);

        return new SerializationFactoryGenerator(messageClass).generateFactory(serializer, deserializer);
    }

    /**
     * Generates a class for registering all generated {@link MessageSerializationFactory} for the current module.
     */
    // TODO: refactor this method to use module names as part of the generated class,
    //  see https://issues.apache.org/jira/browse/IGNITE-14715
    private static TypeSpec generateRegistryInitializer(Map<TypeElement, TypeSpec> factoriesByMessageType) {
        MethodSpec.Builder initializeMethod = MethodSpec.methodBuilder("initialize")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(TypeName.get(MessageSerializationRegistry.class), "serializationRegistry");

        factoriesByMessageType.forEach((messageClass, factory) -> {
            var factoryPackage = ClassName.get(messageClass).packageName();
            var factoryType = ClassName.get(factoryPackage, factory.name);

            initializeMethod.addStatement("serializationRegistry.registerFactory($T.TYPE, new $T())", messageClass, factoryType);
        });

        return TypeSpec.classBuilder("MessageSerializationRegistryInitializer")
            .addModifiers(Modifier.PUBLIC)
            .addMethod(initializeMethod.build())
            .build();
    }

    /**
     * Returns the longest common package name among the given elements' packages.
     */
    private String getParentPackage(Collection<TypeElement> messageClasses) {
        List<String[]> packageNames = messageClasses.stream()
            .map(processingEnv.getElementUtils()::getPackageOf)
            .map(PackageElement::getQualifiedName)
            .map(Name::toString)
            .map(packageName -> packageName.split("\\."))
            .collect(Collectors.toUnmodifiableList());

        int minNameLength = packageNames.stream().mapToInt(arr -> arr.length).min().getAsInt();

        var result = new StringJoiner(".");

        for (int i = 0; i < minNameLength; ++i) {
            var distinctSubPackageNames = new HashSet<String>();

            for (String[] packageName : packageNames) {
                distinctSubPackageNames.add(packageName[i]);
            }

            if (distinctSubPackageNames.size() == 1) {
                result.add(distinctSubPackageNames.iterator().next());
            }
            else {
                break;
            }
        }

        return result.toString();
    }

    /**
     * Writes the given generated class into a file.
     */
    private void writeToFile(String packageName, TypeSpec typeSpec) throws IOException {
        JavaFile
            .builder(packageName, typeSpec)
            .indent(" ".repeat(4))
            .build()
            .writeTo(processingEnv.getFiler());
    }

    /**
     * Checks that the processed annotation is present on a valid element (see the {@link AutoSerializable} annotation
     * javadocs for more details).
     */
    private boolean isValidElement(Element element) {
        if (element.getKind() != ElementKind.INTERFACE) {
            return false;
        }

        var typeUtils = new TypeUtils(processingEnv);

        // perform BFS to find the NetworkMessage interface among all possible superinterfaces
        var queue = new ArrayDeque<Element>();

        queue.add(element);

        while (!queue.isEmpty()) {
            Element currentElement = queue.pop();

            if (typeUtils.isSameType(currentElement.asType(), NetworkMessage.class)) {
                return true;
            }

            ((TypeElement)currentElement).getInterfaces().stream()
                .map(processingEnv.getTypeUtils()::asElement)
                .forEach(queue::add);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }
}
