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
package org.apache.ignite.internal.configuration.processor;

import java.util.Arrays;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import com.squareup.javapoet.ClassName;
import org.apache.ignite.configuration.annotation.DirectAccess;
import org.junit.jupiter.api.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.Compiler.javac;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for basic code generation scenarios.
 */
public class ITProcessorTest extends AbstractProcessorTest {
    /**
     * The simplest test for code generation.
     */
    @Test
    public void testPublicConfigCodeGeneration() {
        final String packageName = "org.apache.ignite.internal.configuration.processor";

        final ClassName testConfigurationSchema = ClassName.get(packageName, "TestConfigurationSchema");

        final BatchCompilation batch = batchCompile(testConfigurationSchema);

        final Compilation status = batch.getCompilationStatus();

        assertEquals(Compilation.Status.SUCCESS, status.status());

        assertEquals(3, batch.generated().size());

        final ConfigSet classSet = batch.getBySchema(testConfigurationSchema);

        assertTrue(classSet.allGenerated());
    }

    /** */
    @Test
    void testSuccessInternalConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        ClassName cls0 = ClassName.get(packageName, "SimpleRootConfigurationSchema");
        ClassName cls1 = ClassName.get(packageName, "SimpleConfigurationSchema");
        ClassName cls2 = ClassName.get(packageName, "ExtendedSimpleRootConfigurationSchema");
        ClassName cls3 = ClassName.get(packageName, "ExtendedSimpleConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls0, cls1, cls2, cls3);

        assertEquals(Compilation.Status.SUCCESS, batchCompile.getCompilationStatus().status());

        assertEquals(4 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls0).allGenerated());
        assertTrue(batchCompile.getBySchema(cls1).allGenerated());
        assertTrue(batchCompile.getBySchema(cls2).allGenerated());
        assertTrue(batchCompile.getBySchema(cls3).allGenerated());
    }

    /** */
    @Test
    void testErrorInternalConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal";

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorInternal0ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorInternal1ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorInternal2ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(
                packageName,
                "SimpleRootConfigurationSchema",
                "SimpleConfigurationSchema",
                "ExtendedSimpleRootConfigurationSchema",
                "ErrorInternal3ConfigurationSchema"
            )
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorInternal4ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(
                packageName,
                "SimpleRootConfigurationSchema",
                "SimpleConfigurationSchema",
                "ErrorInternal5ConfigurationSchema"
            )
        );
    }

    /**
     * Tests that placing the {@link DirectAccess} annotation on fields that represents sub-configurations results in
     * an error.
     */
    @Test
    void testDirectAccessConfiguration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.internal.";

        String source = packageName.replace('.', '/') + "InvalidDirectAccessConfigurationSchema.java";

        Compilation compilation = javac()
            .withProcessors(new Processor())
            .compile(JavaFileObjects.forResource(source));

        assertThat(compilation).hadErrorContaining(
            "@DirectAccess annotation must not be present on nested configuration fields"
        );
    }

    /** */
    @Test
    void testErrorPolymorphicConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.polymorphic";

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic0ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic1ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic2ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic3ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic4ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic5ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic6ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic7ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphic8ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance0ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance1ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance2ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance3ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance4ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance5ConfigurationSchema")
        );

        assertThrows(
            IllegalStateException.class,
            () -> batchCompile(packageName, "ErrorPolymorphicInstance6ConfigurationSchema")
        );
    }

    /** */
    @Test
    void testSuccessPolymorphicConfigCodeGeneration() {
        String packageName = "org.apache.ignite.internal.configuration.processor.polymorphic";

        ClassName cls0 = ClassName.get(packageName, "SimplePolymorphicConfigurationSchema");
        ClassName cls1 = ClassName.get(packageName, "SimplePolymorphicInstanceConfigurationSchema");
        ClassName cls2 = ClassName.get(packageName, "SimpleConfigurationSchema");
        ClassName cls3 = ClassName.get(packageName, "SimpleRootConfigurationSchema");

        BatchCompilation batchCompile = batchCompile(cls0, cls1, cls2, cls3);

        assertEquals(Compilation.Status.SUCCESS, batchCompile.getCompilationStatus().status());

        assertEquals(4 * 3, batchCompile.generated().size());

        assertTrue(batchCompile.getBySchema(cls0).allGenerated());
        assertTrue(batchCompile.getBySchema(cls1).allGenerated());
        assertTrue(batchCompile.getBySchema(cls2).allGenerated());
        assertTrue(batchCompile.getBySchema(cls3).allGenerated());
    }

    /**
     * Compile set of classes.
     *
     * @param packageName Package names.
     * @param classNames Simple class names.
     * @return Result of batch compilation.
     */
    private static BatchCompilation batchCompile(String packageName, String... classNames) {
        ClassName[] classes = Arrays.stream(classNames)
            .map(clsName -> ClassName.get(packageName, clsName))
            .toArray(ClassName[]::new);

        return batchCompile(classes);
    }
}
