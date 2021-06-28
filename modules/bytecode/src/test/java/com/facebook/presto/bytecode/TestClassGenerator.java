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
package com.facebook.presto.bytecode;

import java.io.File;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.ClassGenerator.classGenerator;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClassGenerator {
    @Test
    public void testGenerator()
        throws Exception {
        ClassDefinition classDefinition = new ClassDefinition(
            a(PUBLIC, FINAL),
            "test/Example",
            type(Object.class));

        Parameter argA = arg("a", int.class);
        Parameter argB = arg("b", int.class);

        MethodDefinition method = classDefinition.declareMethod(
            a(PUBLIC, STATIC),
            "add",
            type(int.class),
            List.of(argA, argB));

        method.getBody()
            .append(add(argA, argB))
            .retInt();

        Path tempDir = createTempDirectory("test");

        try {
            StringWriter writer = new StringWriter();

            Class<?> clazz = classGenerator(getClass().getClassLoader())
                .fakeLineNumbers(true)
                .runAsmVerifier(true)
                .dumpRawBytecode(true)
                .outputTo(writer)
                .dumpClassFilesTo(tempDir)
                .defineClass(classDefinition, Object.class);

            Method add = clazz.getMethod("add", int.class, int.class);
            assertEquals(add.invoke(null, 13, 42), 55);

            final String code = writer.toString();

            assertTrue(code.contains("00002 I I  : I I  :     IADD"));
            assertTrue(code.contains("public final class test/Example {"));
            assertTrue(code.contains("// declaration: int add(int, int)"));
            assertTrue(code.contains("LINENUMBER 2002 L1"));

            assertTrue(Files.isRegularFile(tempDir.resolve("test/Example.class")));
        }
        finally {
            deleteDirectory(tempDir.toFile());
        }
    }

    boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
