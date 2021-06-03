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

package org.apache.ignite.network.processor.internal;

import javax.lang.model.element.TypeElement;
import com.squareup.javapoet.ClassName;
import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Wrapper around an element annotated with {@link MessageGroup}.
 */
public class MessageGroupWrapper {
    /** Message group element */
    private final TypeElement element;

    /** Class name of the {@code element}. */
    private final ClassName className;

    /** Annotation present on the {@code element}. */
    private final MessageGroup annotation;

    /**
     * @param messageGroup Element annotated with {@link MessageGroup}.
     */
    public MessageGroupWrapper(TypeElement messageGroup) {
        element = messageGroup;
        className = ClassName.get(messageGroup);
        annotation = messageGroup.getAnnotation(MessageGroup.class);

        if (annotation.groupType() < 0)
            throw new ProcessingException("Group type must not be negative", null, element);
    }

    public TypeElement element() {
        return element;
    }

    /**
     * Returns the package name of the annotated element.
     */
    public String packageName() {
        return className.packageName();
    }

    /**
     * Returns the {@link MessageGroup#groupName()} declared in the annotation.
     */
    public String groupName() {
        return capitalize(annotation.groupName());
    }

    /**
     * Returns the {@link MessageGroup#groupType()} declared in the annotation.
     */
    public short groupType() {
        return annotation.groupType();
    }

    /**
     * Returns the class name of the message factory that should be generated for the current module.
     */
    public ClassName messageFactoryClassName() {
        return ClassName.get(packageName(), groupName() + "Factory");
    }

    /**
     * Creates a copy of the given string with the first letter capitalized.
     */
    private static String capitalize(String str) {
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }
}
