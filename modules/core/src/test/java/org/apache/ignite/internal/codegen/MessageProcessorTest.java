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

package org.apache.ignite.internal.codegen;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.tools.JavaFileObject;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.junit.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class MessageProcessorTest {
    /** */
    @Test
    public void testProcessorGeneratesSerializer() {
        Compilation compilation = compile("TestMessage");

        assertThat(compilation).succeeded();

        checkSerializers(compilation, "TestMessage");
    }

    /** */
    @Test
    public void testEmptyMessage() {
        Compilation compilation = compile("EmptyMessage");

        assertThat(compilation).succeeded();
        assertTrue(compilation.generatedSourceFiles().isEmpty());
    }

    /** */
    @Test
    public void testWrongClassUseOrder() {
        Compilation compilation = compile("WrongClassUseOrder");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testStaticFieldOrderFailed() {
        Compilation compilation = compile("StaticFieldOrder");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testWrongOrderEnumerationFailed() {
        Compilation compilation = compile("WrongOrderEnumeration");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testInheritedMessages() {
        Compilation compilation = compile("AbstractMessage", "ChildMessage");

        assertThat(compilation).succeeded();

        checkSerializers(compilation, "ChildMessage");
    }

    /** */
    @Test
    public void testNestedMessages() {
        Compilation compilation = compile("NestedMessage", "MessageWithNestedMessage", "TestMessageWithNestedMessages");

        assertThat(compilation).succeeded();

        checkSerializers(compilation, "NestedMessage", "MessageWithNestedMessage", "TestMessageWithNestedMessages");
    }

    /** */
    @Test
    public void testMultipleMessages() {
        Compilation compilation = compile("TestMessage", "AbstractMessage", "ChildMessage", "NestedMessage",
            "MessageWithNestedMessage", "TestMessageWithNestedMessages");

        assertThat(compilation).succeeded();

        checkSerializers(compilation, "ChildMessage", "TestMessage", "NestedMessage", "MessageWithNestedMessage",
            "MessageWithNestedMessage");
    }

    /** */
    @Test
    public void testMatrixMessageFailed() {
        Compilation compilation = compile("MatrixMessageMessage");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testPojoFieldFailed() {
        Compilation compilation = compile("PojoFieldMessage");

        assertThat(compilation).failed();
    }

    /** */
    private Compilation compile(String... srcFiles) {
        List<JavaFileObject> input = new ArrayList<>();

        for (String srcFile: srcFiles)
            input.add(javaFile(srcFile));

        File igniteCoreJar = jarForClass(Message.class);
        File igniteCodegenJar = jarForClass(Order.class);

        return Compiler.javac()
            .withClasspath(F.asList(igniteCoreJar, igniteCodegenJar))
            .withProcessors(new MessageProcessor())
            .compile(input);
    }

    /** */
    private JavaFileObject javaFile(String srcName) {
        return JavaFileObjects.forResource("codegen/" + srcName + ".java");
    }

    /** */
    private File jarForClass(Class<?> clazz) {
        try {
            URI jar = clazz
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI();

            return new File(jar);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to locate JAR for: " + clazz.getName(), e);
        }
    }

    /** */
    private void checkSerializers(Compilation compilation, String... msgClasses) {
        assertEquals(msgClasses.length, compilation.generatedSourceFiles().size());

        for (String msgCls : msgClasses) {
            String serCls = msgCls + "Serializer";

            assertThat(compilation)
                .generatedSourceFile("org.apache.ignite.internal.codegen." + serCls)
                .hasSourceEquivalentTo(javaFile(serCls));
        }
    }
}
