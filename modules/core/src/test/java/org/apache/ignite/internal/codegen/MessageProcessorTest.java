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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.QueryIndexMessage;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.mappers.DefaultEnumMapper;
import org.apache.ignite.transactions.TransactionIsolation;
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
            .generatedSourceFile("org.apache.ignite.internal.TestMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestMessageSerializer.java"));
    }

    /** */
    @Test
    public void testCollectionsMessage() {
        Compilation compilation = compile("TestCollectionsMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(1, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestCollectionsMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestCollectionsMessageSerializer.java"));
    }

    /** */
    @Test
    public void testMapMessage() {
        Compilation compilation = compile("TestMapMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(1, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestMapMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestMapMessageSerializer.java"));
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
            .generatedSourceFile("org.apache.ignite.internal.ChildMessageSerializer")
            .hasSourceEquivalentTo(javaFile("ChildMessageSerializer.java"));
    }

    /** */
    @Test
    public void testMultipleMessages() {
        Compilation compilation = compile("TestMessage.java", "AbstractMessage.java", "ChildMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(2, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.ChildMessageSerializer")
            .hasSourceEquivalentTo(javaFile("ChildMessageSerializer.java"));

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestMessageSerializer")
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

    /** Tests {@link QueryIndexMessage} that copies {@link QueryIndex}. */
    @Test
    public void testQueryIndex() {
        Field[] fields0 = QueryIndex.class.getDeclaredFields();

        Map<String, Field> fields = CommonUtils.newHashMap(fields0.length);

        for (Field f : fields0) {
            if (!Modifier.isStatic(f.getModifiers()))
                fields.put(f.getName(), f);
        }

        assertEquals(4, fields.size());

        assertEquals(String.class, fields.get("name").getType());
        assertEquals(LinkedHashMap.class, fields.get("fields").getType());
        assertEquals(QueryIndexType.class, fields.get("type").getType());
        assertEquals(int.class, fields.get("inlineSize").getType());

        QueryIndex idx = new QueryIndex("fld0", QueryIndexType.GEOSPATIAL, false, "testIdx");

        QueryIndexMessage msg = new QueryIndexMessage(idx);

        QueryIndex idx1 = QueryIndexMessage.queryIndex(msg);

        assertEquals(idx, idx1);
    }

    /** */
    @Test
    public void testExceptionFailed() {
        Compilation compilation = compile("ExceptionMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("You should use ErrorMessage for serialization of throwables.");
    }

    /**
     * Positive test for default enum mapper for enum fields.
     */
    @Test
    public void testDefaultMapperEnumFields() {
        Compilation compilation = compile("DefaultMapperEnumFieldsMessage.java");

        assertThat(compilation).succeeded();

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.DefaultMapperEnumFieldsMessageSerializer")
            .hasSourceEquivalentTo(javaFile("DefaultMapperEnumFieldsMessageSerializer.java"));
    }

    /**
     * Negative test for CustomMapper annotation verifying an error is thrown by codegeneration tool if
     * the annotation is used with a field of a primitive type.
     */
    @Test
    public void testCustomMapperCannotBeUsedOnPrimitiveField() {
        Compilation compilation = compile("CustomEnumMapperOnPrimitiveFieldMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("Annotation @CustomMapper must only be used for enum fields.");
    }

    /**
     * Negative test for CustomMapper annotation verifying an error is thrown by codegeneration tool if
     * the annotation is used with a field of an array type.
     */
    @Test
    public void testCustomMapperCannotBeUsedOnArrayField() {
        Compilation compilation = compile("CustomEnumMapperOnArrayFieldMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("Annotation @CustomMapper must only be used for enum fields.");
    }

    /**
     * Positive test for custom EnumMapper implementation for enum field: codegeneration tool
     * generates a serializer using provided EnumMapper implementation.
     * Generated serializer compiles successfully.
     */
    @Test
    public void testCustomMapperEnumFieldsMessage() {
        Compilation compilation = compile("CustomMapperEnumFieldsMessage.java", "TransactionIsolationEnumMapper.java");

        assertThat(compilation).succeeded();

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.CustomMapperEnumFieldsMessageSerializer")
            .hasSourceEquivalentTo(javaFile("CustomMapperEnumFieldsMessageSerializer.java"));
    }

    /** */
    @Test
    public void testMarshallableMessage() {
        Compilation compilation = compile("TestMarshallableMessage.java");

        assertThat(compilation).succeeded();

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestMarshallableMessageMarshallableSerializer")
            .hasSourceEquivalentTo(javaFile("TestMarshallableMessageMarshallableSerializer.java"));
    }

    /**
     * Negative test for a coflict situation when two enum mappers are used for the same enum in different messages.
     */
    @Test
    public void testDifferentMappersForTheSameEnumAreProhibited() {
        Compilation compilation = compile("DefaultMapperEnumFieldsMessage.java",
            "CustomMapperEnumFieldsMessage.java",
            "TransactionIsolationEnumMapper.java");

        assertThat(compilation).failed();

        String errMsg = "Enum " + TransactionIsolation.class.getName() + " is declared with different mappers: " +
            DefaultEnumMapper.class.getName() + " in org.apache.ignite.internal.DefaultMapperEnumFieldsMessage" +
            " and org.apache.ignite.internal.TransactionIsolationEnumMapper in org.apache.ignite.internal.CustomMapperEnumFieldsMessage.";

        assertThat(compilation).hadErrorContaining(errMsg);
    }

    /**
     * Positive test verifies that codegeneration is successful when two messages use DefaultEnumMapper for the same enum type.
     */
    @Test
    public void testDefaultMapperForSameEnumTypeInDifferentMessagesIsAllowed() {
        Compilation compilation = compile("DefaultMapperEnumFieldsMessage.java",
            "DefaultMapperEnumFieldsSecondMessage.java");

        assertThat(compilation).succeeded();
    }

    /**
     * Positive test verifies that codegeneration is successful when two messages use
     * the same custom EnumMapper for the same enum type.
     */
    @Test
    public void testSameCustomMapperForSameEnumTypeInDifferentMessagesIsAllowed() {
        Compilation compilation = compile("CustomMapperEnumFieldsMessage.java",
            "CustomMapperEnumFieldsSecondMessage.java",
            "TransactionIsolationEnumMapper.java");

        assertThat(compilation).succeeded();
    }

    /**
     * Negative test that verifies the compilation failed if the CompressedMessage type is used in Message.
     */
    @Test
    public void testCompressedMessageExplicitUsageFails() {
        String errMsg = "CompressedMessage should not be used explicitly. To compress the required field use the @Compress annotation.";

        Compilation compilation = compile("TestCompressedMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(errMsg);

        compilation = compile("TestCollectionsCompressedMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(errMsg);

        compilation = compile("TestMapCompressedMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(errMsg);
    }

    /** Collection-of-entries encoding of a {@code Map<KeyCacheObject, ?>}: generator recurses into each entry's KCO. */
    @Test
    public void testKeyCacheObjectInCollectionOfEntries() {
        Compilation compilation = compile("KeyCacheObjectEntryMsg.java", "TestKeyCacheObjectCollectionMessage.java");

        assertThat(compilation).succeeded();

        assertEquals(2, compilation.generatedSourceFiles().size());

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.KeyCacheObjectEntryMsgSerializer")
            .hasSourceEquivalentTo(javaFile("KeyCacheObjectEntryMsgSerializer.java"));

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestKeyCacheObjectCollectionMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestKeyCacheObjectCollectionMessageSerializer.java"));
    }

/** {@code @Order Map<KeyCacheObject, GridCacheVersion>}: generator walks keys/values via {@code keySet()/values()}. */
    @Test
    public void testMapWithKeyCacheObjectAndMessageValue() {
        Compilation compilation = compile("TestMapKeyCacheObjectMessage.java");

        assertThat(compilation).succeeded();

        assertThat(compilation)
            .generatedSourceFile("org.apache.ignite.internal.TestMapKeyCacheObjectMessageSerializer")
            .hasSourceEquivalentTo(javaFile("TestMapKeyCacheObjectMessageSerializer.java"));
    }

    /**
     * Negative test that verifies the compilation failed if the Compress annotation is used for unsupported types.
     */
    @Test
    public void testCompressAnnotationFailsForUnsupportedTypes() {
        Compilation compilation = compile("TestCompressUnsupportedTypeMessage.java");

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("Compress annotation is used for an unsupported type: java.util.List");
    }

    /** */
    private Compilation compile(String... srcFiles) {
        return compile(new MessageProcessor(), srcFiles);
    }

    /** */
    static Compilation compile(Processor proc, String... srcFiles) {
        List<JavaFileObject> input = new ArrayList<>();

        for (String srcFile: srcFiles)
            input.add(javaFile(srcFile));

        File igniteCoreJar = jarForClass(Message.class);
        File igniteCodegenJar = jarForClass(Order.class);
        File igniteBinaryApiJar = jarForClass(IgniteUuid.class);
        File igniteCommonsJar = jarForClass(CommonUtils.class);

        return Compiler.javac()
            .withClasspath(F.asList(igniteCoreJar, igniteCodegenJar, igniteBinaryApiJar, igniteCommonsJar))
            .withProcessors(proc)
            .compile(input);
    }

    /** */
    static JavaFileObject javaFile(String srcName) {
        return JavaFileObjects.forResource("codegen/" + srcName);
    }

    /** */
    private static File jarForClass(Class<?> clazz) {
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
}
