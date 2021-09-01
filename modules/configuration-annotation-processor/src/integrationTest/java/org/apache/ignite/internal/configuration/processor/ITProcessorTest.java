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
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

        assertNotEquals(Compilation.Status.FAILURE, status.status());

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

        assertNotEquals(Compilation.Status.FAILURE, batchCompile.getCompilationStatus().status());

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
     * Compile set of classes.
     *
     * @param packageName Package names.
     * @param classNames Simple class names.
     * @return Result of batch compilation.
     */
    private BatchCompilation batchCompile(String packageName, String... classNames) {
        ClassName[] classes = Arrays.stream(classNames)
            .map(clsName -> ClassName.get(packageName, clsName))
            .toArray(ClassName[]::new);

        return batchCompile(classes);
    }
}
