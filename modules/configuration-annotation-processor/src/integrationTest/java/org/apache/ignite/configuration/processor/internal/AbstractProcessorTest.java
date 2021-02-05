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
package org.apache.ignite.configuration.processor.internal;

import com.google.common.base.Functions;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import com.squareup.javapoet.ClassName;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.tools.JavaFileObject;

import static com.google.testing.compile.Compiler.javac;

/**
 * Base class for configuration annotation processor tests.
 */
public class AbstractProcessorTest {

    /**
     * Compile set of classes
     * @param schemaClasses Configuration schema classes.
     * @return Result of batch compilation.
     */
    protected static BatchCompilation batchCompile(ClassName... schemaClasses) {
        List<String> fileNames = Arrays.stream(schemaClasses)
            .map(name -> {
                final String folderName = name.packageName().replace(".", "/");
                return String.format("%s/%s.java", folderName, name.simpleName());
            })
            .collect(Collectors.toList());

        final List<JavaFileObject> fileObjects = fileNames.stream().map(JavaFileObjects::forResource).collect(Collectors.toList());

        final Compilation compilation = javac()
            .withProcessors(new Processor())
            .compile(fileObjects);

        return new BatchCompilation(Arrays.asList(schemaClasses), compilation);
    }

    /**
     * Get {@link ConfigSet} object from generated classes.
     * @param clazz Configuration schema ClassName.
     * @param generatedClasses Map with all generated classes.
     * @return ConfigSet.
     */
    protected static ConfigSet getConfigSet(ClassName clazz, final Map<ClassName, JavaFileObject> generatedClasses) {
        final ClassName configurationName = Utils.getConfigurationName(clazz);
        final ClassName viewName = Utils.getViewName(clazz);
        final ClassName initName = Utils.getInitName(clazz);
        final ClassName changeName = Utils.getChangeName(clazz);

        final JavaFileObject configurationFileObject = generatedClasses.get(configurationName);
        final JavaFileObject viewClass = generatedClasses.get(viewName);
        final JavaFileObject initClass = generatedClasses.get(initName);
        final JavaFileObject changeClass = generatedClasses.get(changeName);

        return new ConfigSet(configurationFileObject, viewClass, initClass, changeClass);
    }

    /**
     * Get {@link ClassName} object from generated file path.
     * @param fileName File path.
     * @return ClassName.
     */
    protected static ClassName fromGeneratedFilePath(String fileName) {
        final String filePath = fileName.replace("/SOURCE_OUTPUT/", "");
        return fromFilePath(filePath);
    }

    /**
     * Get {@link ClassName} object from file path.
     * @param fileName File path.
     * @return ClassName.
     */
    protected static ClassName fromFilePath(String fileName) {
        int slashIdx = fileName.lastIndexOf("/");
        int dotJavaIdx = fileName.lastIndexOf(".java");

        String packageName = fileName.substring(0, slashIdx).replace("/", ".");

        final String className = fileName.substring(slashIdx + 1, dotJavaIdx);

        return ClassName.get(packageName, className);
    }

    /**
     * Result of multiple compiled schema classes.
     */
    protected static class BatchCompilation {
        /** Generated source files. */
        private final List<JavaFileObject> generatedSources;

        /** Generated classes mapped by config schema class name. */
        private final Map<ClassName, JavaFileObject> generatedClasses;

        /** Config class sets by config schema class name. */
        private final Map<ClassName, ConfigSet> classSets;

        /** Compilation status. */
        private final Compilation compilationStatus;

        /**
         * Constructor.
         * @param schemaClasses List of schema class names.
         * @param compilation Compilation status.
         */
        public BatchCompilation(List<ClassName> schemaClasses, Compilation compilation) {
            this.compilationStatus = compilation;

            generatedSources = compilation.generatedSourceFiles();

            generatedClasses = generatedSources.stream()
                .collect(Collectors.toMap(object -> fromGeneratedFilePath(object.getName()), Functions.identity()));

            classSets = schemaClasses.stream().collect(
                Collectors.toMap(name -> name, name -> getConfigSet(name, generatedClasses))
            );
        }

        /**
         * Get config class set by schema class name.
         * @param schemaClass Schema class name.
         * @return Config class set.
         */
        public ConfigSet getBySchema(ClassName schemaClass) {
            return classSets.get(schemaClass);
        }

        /**
         * Get compilation status.
         * @return Compilation status.
         */
        public Compilation getCompilationStatus() {
            return compilationStatus;
        }

        /**
         * Get all generated source files.
         * @return Generated source files.
         */
        public List<JavaFileObject> generated() {
            return generatedSources;
        }
    }

}
