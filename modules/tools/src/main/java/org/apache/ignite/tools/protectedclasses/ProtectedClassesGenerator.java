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

package org.apache.ignite.tools.protectedclasses;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.TreeSet;
import io.github.classgraph.ClassGraph;

/**
 * Generates a sorted list of protected classes — classes from {@code Message} and {@code IgniteDataTransferObject}
 * hierarchies, as well as classes containing {@code @Order}-annotated fields, that are part of the wire protocol
 * and require special review when modified.
 * <p>
 * Usage: {@code java ProtectedClassesGenerator <output-file>}
 */
public class ProtectedClassesGenerator {
    /** Base interface for communication messages. */
    private static final String MESSAGE_INTERFACE = "org.apache.ignite.plugin.extensions.communication.Message";

    /** Base class for data transfer objects. */
    private static final String IDTO_CLASS = "org.apache.ignite.internal.dto.IgniteDataTransferObject";

    /** Annotation that marks serialized fields in Message classes. */
    private static final String ORDER_ANNOTATION = "org.apache.ignite.internal.Order";

    /** @param args */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: ProtectedClassesGenerator <output-file>");
            System.exit(1);
        }

        var outputPath = Paths.get(args[0]);

        var protectedClasses = new TreeSet<String>();

        try (var scanResult = new ClassGraph()
            .enableClassInfo()
            .enableFieldInfo()
            .enableAnnotationInfo()
            .acceptPackages("org.apache.ignite")
            .scan()
        ) {
            scanResult.getClassesImplementing(MESSAGE_INTERFACE)
                .forEach(cls -> protectedClasses.add(cls.getName()));

            scanResult.getSubclasses(IDTO_CLASS)
                .forEach(cls -> protectedClasses.add(cls.getName()));

            // Include classes that have fields annotated with @Order.
            scanResult.getClassesWithFieldAnnotation(ORDER_ANNOTATION)
                .forEach(cls -> protectedClasses.add(cls.getName()));

            // Include the base classes themselves.
            protectedClasses.add(MESSAGE_INTERFACE);
            protectedClasses.add(IDTO_CLASS);
        }

        try (var writer = new PrintWriter(outputPath.toFile())) {
            writer.println("# Auto-generated list of protected classes (Message/IgniteDataTransferObject hierarchy).");
            writer.println("# Changes to these classes require special review.");
            writer.println("# Regenerated automatically after merging PRs with 'protected-classes' label.");

            for (var clazz : protectedClasses) {
                writer.println(clazz);
            }
        }

        System.out.println("Generated " + outputPath + " with " + protectedClasses.size() + " protected classes.");
    }
}
