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

package org.apache.ignite.internal.managers.systemview.walker;

import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

/**
 * Generates {@code SystemViewRowAttributeWalker} implementations
 * for view classes marked by {@code SystemViewDescriptor} interface.
 * <p>
 * The generated walker follows the naming convention:
 * {@code org.apache.ignite.internal.managers.systemview.walker.codegen.[ViewClassName]Walker}.
 */
@SupportedAnnotationTypes("org.apache.ignite.internal.managers.systemview.walker.Order")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class SystemViewRowAttributeWalkerProcessor extends AbstractProcessor {
    /** Base interface that every message must implement. */
    static final String VIEW_INTERFACE = "org.apache.ignite.internal.managers.systemview.SystemViewDescriptor";

    /**
     * Processes all classes implementing the {@code SystemViewDescriptor} interface and generates corresponding walker code.
     */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        TypeMirror viewType = processingEnv.getElementUtils().getTypeElement(VIEW_INTERFACE).asType();

        for (Element el : roundEnv.getRootElements()) {
            if (el.getKind() != ElementKind.CLASS)
                continue;

            TypeElement clazz = (TypeElement)el;

            if (!processingEnv.getTypeUtils().isAssignable(clazz.asType(), viewType))
                continue;

            if (clazz.getModifiers().contains(Modifier.ABSTRACT))
                continue;

            try {
                new SystemViewRowAttributeWalkerGenerator(processingEnv).generate(clazz);
            }
            catch (Exception e) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate a message serializer:" + e.getMessage(),
                    clazz);
            }
        }

        return true;
    }
}
