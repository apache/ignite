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

package org.apache.ignite.internal.testframework;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension for injecting temporary folders into test classes.
 * <p>
 * This extension supports both field and parameter injection of {@link Path} parameters annotated with the
 * {@link WorkDirectory} annotation.
 * <p>
 * A new temporary folder is created for every test method and will be located relative to the module,
 * where the tests are being run, by the following path: "target/work/{@literal <name-of-the-test-method>}".
 * It is removed after a test has finished running, but this behaviour can be controlled by setting the
 * {@link WorkDirectoryExtension#KEEP_WORK_DIR_PROPERTY} property to {@code true}, in which case the created folder can
 * be kept intact for debugging purposes.
 */
public class WorkDirectoryExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {
    /**
     * System property that, when set to {@code true}, will make the extension preserve the created directories.
     * Default is {@code false}.
     */
    public static final String KEEP_WORK_DIR_PROPERTY = "KEEP_WORK_DIR";

    /** Base path for all temporary folders in a module. */
    private static final Path BASE_PATH = Path.of("target", "work");

    /** {@inheritDoc} */
    @Override public void beforeEach(ExtensionContext context) throws Exception {
        Object testInstance = context.getRequiredTestInstance();

        Field workDirField = getWorkDirField(testInstance.getClass());

        if (workDirField == null)
            return;

        workDirField.setAccessible(true);

        workDirField.set(testInstance, createWorkDir(context));
    }

    /** {@inheritDoc} */
    @Override public void afterEach(ExtensionContext context) throws Exception {
        if (shouldRemoveDir())
            IgniteUtils.deleteIfExists(BASE_PATH);
    }

    /** {@inheritDoc} */
    @Override public boolean supportsParameter(
        ParameterContext parameterContext, ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return getParameterType(parameterContext).equals(Path.class)
            && parameterContext.isAnnotated(WorkDirectory.class);
    }

    /** {@inheritDoc} */
    @Override public Object resolveParameter(
        ParameterContext parameterContext, ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        try {
            return createWorkDir(extensionContext);
        }
        catch (IOException e) {
            throw new ParameterResolutionException("Error when creating the work directory", e);
        }
    }

    /**
     * Shortcut for extracting the method parameter type from a {@link ParameterContext}.
     */
    private static Class<?> getParameterType(ParameterContext parameterContext) {
        return parameterContext.getParameter().getType();
    }

    /**
     * Creates the temporary folder for the given test method.
     */
    private static Path createWorkDir(ExtensionContext extensionContext) throws IOException {
        if (shouldRemoveDir())
            IgniteUtils.deleteIfExists(BASE_PATH);

        Path workDir = BASE_PATH.resolve(extensionContext.getRequiredTestMethod().getName());

        Files.createDirectories(workDir);

        return workDir;
    }

    /**
     * Looks for the annotated field inside the given test class.
     *
     * @return annotated field or {@code null} if no fields have been found
     * @throws IllegalStateException if more than one annotated fields have been found
     */
    @Nullable
    private static Field getWorkDirField(Class<?> testClass) {
        List<Field> fields = AnnotationSupport.findAnnotatedFields(
            testClass,
            WorkDirectory.class,
            field -> field.getType().equals(Path.class),
            HierarchyTraversalMode.TOP_DOWN
        );

        if (fields.isEmpty())
            return null;

        if (fields.size() != 1) {
            throw new IllegalStateException(String.format(
                "Test class must have a single field of type 'java.nio.file.Path' annotated with '@WorkDirectory', " +
                    "but %d fields have been found",
                fields.size()
            ));
        }

        return fields.get(0);
    }

    /**
     * Returns {@code true} if the extension should remove the created directories.
     */
    private static boolean shouldRemoveDir() {
        return !IgniteSystemProperties.getBoolean(KEEP_WORK_DIR_PROPERTY);
    }
}
