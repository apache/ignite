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

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension for injecting temporary folders into test classes.
 *
 * <p>This extension supports both field and parameter injection of {@link Path} parameters annotated with the {@link WorkDirectory}
 * annotation.
 *
 * <p>A new temporary folder can be created for every test method (when used as a test parameter or as a member field) or a single time in
 * a
 * test class' lifetime (when used as a parameter in a {@link BeforeAll} hook or as a static field). Temporary folders are located relative
 * to the module, where the tests are being run, and their paths depends on the lifecycle of the folder:
 *
 * <ol>
 *     <li>For test methods: "target/work/{@literal <name-of-the-test-class>/<name-of-the-test-method>_<current_time_millis>}"</li>
 *     <li>For test classes: "target/work/{@literal <name-of-the-test-class>/static_<current_time_millis>}"</li>
 * </ol>
 *
 * <p>Temporary folders are removed after tests have finished running, but this behaviour can be controlled by setting the
 * {@link WorkDirectoryExtension#KEEP_WORK_DIR_PROPERTY} property to {@code true}, in which case the created folder can
 * be kept intact for debugging purposes.
 */
public class WorkDirectoryExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(WorkDirectoryExtension.class);

    /**
     * System property that, when set to {@code true}, will make the extension preserve the created directories. Default is {@code false}.
     */
    public static final String KEEP_WORK_DIR_PROPERTY = "KEEP_WORK_DIR";

    /** Base path for all temporary folders in a module. */
    private static final Path BASE_PATH = Path.of("target", "work");

    /** Name of the work directory that will be injected into {@link BeforeAll} methods or static members. */
    private static final String STATIC_FOLDER_NAME = "static";

    /**
     * Creates and injects a temporary directory into a static field.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        Field workDirField = getWorkDirField(context);

        if (workDirField == null || !Modifier.isStatic(workDirField.getModifiers())) {
            return;
        }

        workDirField.setAccessible(true);

        workDirField.set(null, createWorkDir(context));
    }

    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        removeWorkDir(context);

        Path testClassDir = getTestClassDir(context);

        // remove the folder for the current test class, if empty
        if (isEmpty(testClassDir)) {
            IgniteUtils.deleteIfExists(testClassDir);
        }

        // remove the base folder, if empty
        if (isEmpty(BASE_PATH)) {
            IgniteUtils.deleteIfExists(BASE_PATH);
        }
    }

    /**
     * Creates and injects a temporary directory into a field.
     */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Field workDirField = getWorkDirField(context);

        if (workDirField == null || Modifier.isStatic(workDirField.getModifiers())) {
            return;
        }

        workDirField.setAccessible(true);

        workDirField.set(context.getRequiredTestInstance(), createWorkDir(context));
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        removeWorkDir(context);
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(Path.class)
                && parameterContext.isAnnotated(WorkDirectory.class);
    }

    /** {@inheritDoc} */
    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        if (getWorkDirField(extensionContext) != null) {
            throw new IllegalStateException(
                    "Cannot perform parameter injection, because there exists a field annotated with @WorkDirectory"
            );
        }

        try {
            return createWorkDir(extensionContext);
        } catch (IOException e) {
            throw new ParameterResolutionException("Error when creating the work directory", e);
        }
    }

    /**
     * Creates a temporary folder for the given test method.
     */
    private static Path createWorkDir(ExtensionContext context) throws IOException {
        Path existingDir = context.getStore(NAMESPACE).get(context.getUniqueId(), Path.class);

        if (existingDir != null) {
            return existingDir;
        }

        String testMethodDir = context.getTestMethod()
                .map(Method::getName)
                .orElse(STATIC_FOLDER_NAME);

        Path workDir = getTestClassDir(context).resolve(testMethodDir + '_' + System.currentTimeMillis());

        Files.createDirectories(workDir);

        context.getStore(NAMESPACE).put(context.getUniqueId(), workDir);

        return workDir;
    }

    /**
     * Returns a path to the working directory of the test class (identified by the JUnit context).
     */
    private static Path getTestClassDir(ExtensionContext context) {
        return BASE_PATH.resolve(context.getRequiredTestClass().getSimpleName());
    }

    /**
     * Removes a previously created work directory.
     */
    private static void removeWorkDir(ExtensionContext context) {
        Path workDir = context.getStore(NAMESPACE).remove(context.getUniqueId(), Path.class);

        if (workDir != null && shouldRemoveDir()) {
            IgniteUtils.deleteIfExists(workDir);
        }
    }

    /**
     * Looks for the annotated field inside the given test class.
     *
     * @return Annotated field or {@code null} if no fields have been found
     * @throws IllegalStateException If more than one annotated fields have been found
     */
    @Nullable
    private static Field getWorkDirField(ExtensionContext context) {
        List<Field> fields = AnnotationSupport.findAnnotatedFields(
                context.getRequiredTestClass(),
                WorkDirectory.class,
                field -> field.getType().equals(Path.class),
                HierarchyTraversalMode.TOP_DOWN
        );

        if (fields.isEmpty()) {
            return null;
        }

        if (fields.size() != 1) {
            throw new IllegalStateException(String.format(
                    "Test class must have a single field of type 'java.nio.file.Path' annotated with '@WorkDirectory', "
                            + "but %d fields have been found",
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

    /**
     * Returns {@code true} if the given directory is empty or {@code false} if the given directory contains files or does not exist.
     */
    private static boolean isEmpty(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return false;
        }

        try (Stream<Path> list = Files.list(dir)) {
            return list.findAny().isEmpty();
        }
    }
}
