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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import javax.tools.JavaFileObject;
import org.apache.commons.io.IOUtils;
import spoon.Launcher;
import spoon.SpoonException;
import spoon.compiler.SpoonResource;
import spoon.reflect.declaration.CtClass;
import spoon.reflect.declaration.CtType;
import spoon.support.compiler.VirtualFile;

/**
 * Wrapper for generated classes of the configuration schema.
 */
public class ConfigSet {
    /** VIEW class. */
    private final JavaFileObject viewClass;

    /** CHANGE class. */
    private final JavaFileObject changeClass;

    /** Constructor. */
    public ConfigSet(JavaFileObject viewClass, JavaFileObject changeClass) {
        this.viewClass = viewClass;
        this.changeClass = changeClass;
    }

    /**
     * Parse source file.
     * @param clz Class file object.
     * @return Parsed class.
     */
    private ParsedClass parse(JavaFileObject clz) {
        String classFileContent;
        try {
            classFileContent = IOUtils.toString(clz.openInputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse class: " + e.getMessage(), e);
        }

        return new ParsedClass(parseClass(classFileContent));
    }

    /**
     * @return {@code true} if all required classes were generated.
     */
    public boolean allGenerated() {
        return viewClass != null && changeClass != null;
    }

    /**
     * Butchered version of {@link Launcher#parseClass(String)}, because {@code spoon} is such a garbage it can't even
     * parse valid classes without issues.
     *
     * @param code Code.
     * @return AST.
     */
    private static CtClass<?> parseClass(String code) {
        Launcher launcher = new Launcher();
        launcher.addInputResource((SpoonResource)(new VirtualFile(code)));
        launcher.getEnvironment().setNoClasspath(true);
        launcher.getEnvironment().setAutoImports(true);
        Collection<CtType<?>> allTypes = launcher.buildModel().getAllTypes();

        // This is how we do "getLast" for streams. Pretty bad, I know.
        return (CtClass)allTypes.stream().reduce((fst, snd) -> snd).orElseThrow(SpoonException::new);
    }
}
