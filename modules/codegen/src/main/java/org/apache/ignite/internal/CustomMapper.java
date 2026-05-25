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

package org.apache.ignite.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to specify a custom mapping strategy for an enum type during code generation.
 * It allows associating a custom mapper class that defines how enum constants are serialized and deserialized.
 *
 * <p>It is used in conjunction with {@link Order} annotation and used to mark fields of enum type
 * that should be serialized and deserialized using custom logic.</p>
 *
 * <p>The class specified by {@link #value()} must implement {@code org.apache.ignite.plugin.extensions.communication.mappers.CustomMapper}
 * interface to provide methods for converting enum values to and from their external representations.</p>
 *
 * @see #value()
 * @see org.apache.ignite.internal.Order
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.FIELD)
public @interface CustomMapper {
    /**
     * Returns the fully qualified class name of the custom mapper implementation
     * used to handle the enum's value conversion.
     *
     * <p>The specified class must be available on the classpath during code generation
     * and must adhere to the expected mapper interface contract.</p>
     *
     * @return The fully qualified name of the mapper class. Must not be null or empty.
     */
    String value() default "";
}
