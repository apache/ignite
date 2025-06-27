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

import java.util.ArrayList;
import java.util.List;
import javax.tools.JavaFileObject;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.apache.ignite.internal.MessageProcessor;
import org.junit.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class MessageProcessorTest {
    /** */
    @Test
    public void testProcessorGeneratesSerializer() {
        Compilation compilation = compile("TestMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(1, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.codegen.TestMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestMessageSerializer.java"));
    }

    /** */
    @Test
    public void testEmptyMessage() {
        Compilation compilation = compile("EmptyMessage.java");

        assertThat(compilation).succeeded();
        assertTrue(compilation.generatedSourceFiles().isEmpty());
    }

    /** */
    @Test
    public void testWrongClassUseOrder() {
        Compilation compilation = compile("WrongClassUseOrder.java");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testStaticFieldOrderFailed() {
        Compilation compilation = compile("StaticFieldOrder.java");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testWrongOrderEnumerationFailed() {
        Compilation compilation = compile("WrongOrderEnumeration.java");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testInheritedMessages() {
        Compilation compilation = compile("AbstractMessage.java", "ChildMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(1, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.codegen.ChildMessageSerializer")
            .hasSourceEquivalentTo(javaFile("ChildMessageSerializer.java"));
    }

    /** */
    @Test
    public void testMultipleMessages() {
        Compilation compilation = compile("TestMessage.java", "AbstractMessage.java", "ChildMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(2, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.codegen.ChildMessageSerializer")
            .hasSourceEquivalentTo(javaFile("ChildMessageSerializer.java"));

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.codegen.TestMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestMessageSerializer.java"));
    }

    /** */
    @Test
    public void testMatrixMessageFailed() {
        Compilation compilation = compile("MatrixMessageMessage.java");

        assertThat(compilation).failed();
    }

    /** */
    @Test
    public void testPojoFieldFailed() {
        Compilation compilation = compile("PojoFieldMessage.java");

        assertThat(compilation).failed();
    }

    /** */
    private Compilation compile(String... srcFiles) {
        List<JavaFileObject> input = new ArrayList<>();

        for (String srcFile: srcFiles)
            input.add(javaFile(srcFile));

        return Compiler.javac()
            .withProcessors(new MessageProcessor())
            .compile(input);
    }

    /** */
    private JavaFileObject javaFile(String srcName) {
        return JavaFileObjects.forResource("codegen/" + srcName);
    }
}
