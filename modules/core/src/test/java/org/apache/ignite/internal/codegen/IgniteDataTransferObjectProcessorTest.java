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

import com.google.testing.compile.Compilation;
import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.idto.IgniteDataTransferObjectProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static javax.tools.StandardLocation.CLASS_OUTPUT;
import static org.apache.ignite.internal.codegen.MessageProcessorTest.compile;
import static org.apache.ignite.internal.codegen.MessageProcessorTest.javaFile;
import static org.apache.ignite.internal.codegen.MessageProcessorTest.jsonFile;
import static org.junit.Assert.assertEquals;

/** */
public class IgniteDataTransferObjectProcessorTest {
    /** */
    @Test
    public void testProcessorGeneratesSerializer() {
        Compilation compilation = compile(new IgniteDataTransferObjectProcessor(), "idto/TestIgniteDataTransferObject.java");

        assertThat(compilation).succeeded();

        assertEquals(2, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestIgniteDataTransferObjectSerializer")
            .hasSourceEquivalentTo(javaFile("idto/TestIgniteDataTransferObjectSerializer.java"));
    }

    /** */
    @Test
    public void testProcessorGeneratesJson() {
        Compilation compilation = compile(new IgniteDataTransferObjectProcessor(),
            "idto/TestIgniteDataTransferObject.java", "idto/TestInheritedIgniteDataTransferObject.java");

        assertThat(compilation).succeeded();

        assertJson(compilation, "TestIgniteDataTransferObject", "TestIgniteDataTransferObject.json");
        assertJson(compilation, "TestInheritedIgniteDataTransferObject", "TestInheritedIgniteDataTransferObject.json");
        assertJson(compilation, "TestInheritedIgniteDataTransferObject$Nested", "TestInheritedIgniteDataTransferObjectNested.json");
        assertEnumJson(compilation);
    }

    /** */
    @Test
    public void testEnumSharedWithMessage() {
        Compilation compilation = compile(F.asList(new MessageProcessor(), new IgniteDataTransferObjectProcessor()),
            "TestEnumSetMessage.java", "idto/TestIgniteDataTransferObject.java", "idto/TestInheritedIgniteDataTransferObject.java");

        assertThat(compilation).succeeded();

        assertThat(compilation)
            .generatedFile(CLASS_OUTPUT, "META-INF/ignite-wire/messages/org.apache.ignite.internal.TestEnumSetMessage.json")
            .contentsAsUtf8String().isEqualTo(jsonFile("TestEnumSetMessage.json"));

        assertJson(compilation, "TestInheritedIgniteDataTransferObject", "TestInheritedIgniteDataTransferObject.json");
        assertEnumJson(compilation);
    }

    /** */
    @Test
    public void testWrongOrderEnumerationFailed() {
        Compilation compilation = compile(new IgniteDataTransferObjectProcessor(), "idto/WrongOrderIgniteDataTransferObject.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("Annotation @Order must be a sequence from 0 to 1 [field=str");
    }

    /** */
    private void assertJson(Compilation compilation, String cls, String expected) {
        assertThat(compilation)
            .generatedFile(CLASS_OUTPUT, "META-INF/ignite-wire/idto/org.apache.ignite.internal." + cls + ".json")
            .contentsAsUtf8String().isEqualTo(jsonFile("idto/" + expected));
    }

    /** */
    private void assertEnumJson(Compilation compilation) {
        assertThat(compilation)
            .generatedFile(CLASS_OUTPUT, "META-INF/ignite-wire/enums/org.apache.ignite.transactions.TransactionIsolation.json")
            .contentsAsUtf8String().isEqualTo(jsonFile("TransactionIsolation.json"));
    }
}
